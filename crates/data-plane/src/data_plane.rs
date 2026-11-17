//! Child-side data-plane session, per-operation actors, and native API.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Weak};

use protocol_telemetry::{
    ActorType, Outcome, Phase, Protocol, Reason, TransitionEmitter, TransitionRecord,
};
use swactor::actor::{ActorAddress, ActorExited, ActorInterface, Ctx};
use swactor::runtime::Runtime;
use swactor::std::CtxWatching;
use swactor_engine::ActorCompletion;

use crate::blob::{
    Blob, BlobLease, BlobMetadata, BlobView, LeaseReleaser, WritableArenaView, WritableBlobLease,
    WritableViewObserver,
};
use crate::byte_ring::{
    Endpoint, FlowError, HeaderError, PeerTermination, RecordCursor, RecordKind, RingHandle, Role,
    attach_mapped,
};
use crate::mapped_arena::MappedArena;
use crate::namespace::StreamIncarnation;
use crate::path::DataPath;
pub use crate::protocol::{
    AccessMode, BlobAllocation, DataPlaneError, DescriptorCapabilities, DescriptorKind, Errno,
    NamespaceOperationResult, OpenOptions,
};
use crate::protocol::{
    ChildSessionIn, HostSessionIn, HostStreamIn, NamespaceOperation, OpenPolicy, SessionCapability,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildSessionState {
    Attaching,
    Running,
    Closing,
    Closed,
}

fn operation_outcome<T>(result: &Result<T, DataPlaneError>) -> Outcome {
    match result {
        Ok(_) => Outcome::Success,
        Err(DataPlaneError::OperationCancelled) => Outcome::Cancelled,
        Err(DataPlaneError::PeerLost) => Outcome::PeerLost,
        Err(DataPlaneError::PathReplaced(_)) => Outcome::Replaced,
        Err(_) => Outcome::Failed,
    }
}

pub struct DataPlaneBootstrap {
    pub arena: Arc<MappedArena>,
    pub data_plane: DataPlane,
}

/// Observes terminal loss of one concrete host actor route, not transient
/// transport disconnection. Registration must report an already-terminal route
/// synchronously, so loss cannot race the attachment request.
pub trait HostSessionRouteObserver: Send + Sync {
    fn watch_terminal(
        &self,
        host: ActorAddress,
        terminal: Arc<dyn Fn() + Send + Sync>,
    ) -> HostSessionRouteWatch;
}

/// Keeps a native route observation registered until the last DataPlane clone
/// is dropped. The subscription's Drop implementation unregisters the observer.
pub struct HostSessionRouteWatch {
    _subscription: Box<dyn Send + Sync>,
}

impl HostSessionRouteWatch {
    pub fn new(subscription: impl Send + Sync + 'static) -> Self {
        Self {
            _subscription: Box::new(subscription),
        }
    }
}

struct AttachmentObservation {
    child_node: Option<[u8; 32]>,
    transitions: Option<TransitionEmitter>,
    route_observer: Option<Arc<dyn HostSessionRouteObserver>>,
}

impl DataPlaneBootstrap {
    pub async fn attach(
        arena_fd: OwnedFd,
        runtime: Runtime,
        host_session: ActorAddress,
        session_capability: SessionCapability,
        transitions: Option<TransitionEmitter>,
    ) -> Result<Self, DataPlaneError> {
        let (arena, resolved) = Self::map_arena(arena_fd)?;
        Self::attach_mapped(
            arena,
            resolved,
            runtime,
            host_session,
            session_capability,
            None,
            transitions,
        )
        .await
    }

    pub fn map_arena(
        arena_fd: OwnedFd,
    ) -> Result<(Arc<MappedArena>, crate::bootstrap::ResolvedBootstrap), DataPlaneError> {
        let (arena, resolved) = MappedArena::map(arena_fd)
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        Ok((Arc::new(arena), resolved))
    }

    /// Attach through a route whose terminal owner may be outside this runtime.
    /// The route observer is required: local actor watches cannot detect a
    /// remote process disappearing. It supplies the fenced
    /// `ChildSessionIn::HostUnavailable` event for terminal route loss.
    pub async fn attach_routed(
        arena_fd: OwnedFd,
        runtime: Runtime,
        host_session: ActorAddress,
        session_capability: SessionCapability,
        child_node: Option<[u8; 32]>,
        transitions: Option<TransitionEmitter>,
        route_observer: Arc<dyn HostSessionRouteObserver>,
    ) -> Result<Self, DataPlaneError> {
        let (arena, resolved) = Self::map_arena(arena_fd)?;
        Self::attach_mapped_observed(
            arena,
            resolved,
            runtime,
            host_session,
            session_capability,
            AttachmentObservation {
                child_node,
                transitions,
                route_observer: Some(route_observer),
            },
        )
        .await
    }

    pub async fn attach_mapped(
        arena: Arc<MappedArena>,
        resolved: crate::bootstrap::ResolvedBootstrap,
        runtime: Runtime,
        host_session: ActorAddress,
        session_capability: SessionCapability,
        child_node: Option<[u8; 32]>,
        transitions: Option<TransitionEmitter>,
    ) -> Result<Self, DataPlaneError> {
        Self::attach_mapped_observed(
            arena,
            resolved,
            runtime,
            host_session,
            session_capability,
            AttachmentObservation {
                child_node,
                transitions,
                route_observer: None,
            },
        )
        .await
    }

    async fn attach_mapped_observed(
        arena: Arc<MappedArena>,
        resolved: crate::bootstrap::ResolvedBootstrap,
        runtime: Runtime,
        host_session: ActorAddress,
        session_capability: SessionCapability,
        observation: AttachmentObservation,
    ) -> Result<Self, DataPlaneError> {
        let AttachmentObservation {
            child_node,
            transitions,
            route_observer,
        } = observation;
        if route_observer.is_none() && !runtime.is_local_actor(host_session) {
            return Err(DataPlaneError::InvalidArgument(
                "remote host attachment requires a terminal route observer".to_owned(),
            ));
        }
        let attached = runtime
            .new_inbox::<Result<u64, DataPlaneError>>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let attach_reply = *attached.addr();
        let child = runtime
            .spawn(ChildDataPlaneSessionActor {
                runtime: runtime.clone(),
                host_session,
                arena_generation: resolved.arena_generation,
                session_capability,
                child_node,
                session_generation: None,
                attach_reply: Some(attach_reply),
                operations: HashMap::new(),
                namespace_operations: HashSet::new(),
                state: ChildSessionState::Attaching,
                stream_waiters: HashMap::new(),
                pending_blob_releases: HashSet::new(),
                deferred_blob_opens: VecDeque::new(),
                close_replies: Vec::new(),
                close_result: None,
                host_close_result: None,
                transitions: transitions.clone(),
            })
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;

        let mut cancellation = AttachmentCancellation {
            runtime: runtime.clone(),
            child,
            armed: true,
        };
        let route_watch = route_observer.map(|observer| {
            let sender = runtime.clone();
            Arc::new(observer.watch_terminal(
                host_session,
                Arc::new(move || {
                    let _ = sender.send_to(child, ChildSessionIn::HostUnavailable { host_session });
                }),
            ))
        });
        attached.recv().await?;
        cancellation.armed = false;
        Ok(Self {
            arena: arena.clone(),
            data_plane: DataPlane {
                runtime,
                child_session: child,
                arena,
                transitions,
                _route_watch: route_watch,
            },
        })
    }
}

struct AttachmentCancellation {
    runtime: Runtime,
    child: ActorAddress,
    armed: bool,
}

impl Drop for AttachmentCancellation {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.runtime.stop_actor(self.child);
        }
    }
}

struct DescriptorOpenCancellation {
    runtime: Runtime,
    child_session: ActorAddress,
    reply_to: ActorAddress,
    armed: bool,
}

impl Drop for DescriptorOpenCancellation {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.runtime.send_to(
                self.child_session,
                ChildSessionIn::CancelOpen {
                    reply_to: self.reply_to,
                },
            );
        }
    }
}

struct NamespaceOperationCancellation {
    runtime: Runtime,
    child_session: ActorAddress,
    reply_to: ActorAddress,
    armed: bool,
}

impl Drop for NamespaceOperationCancellation {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.runtime.send_to(
                self.child_session,
                ChildSessionIn::CancelNamespace {
                    reply_to: self.reply_to,
                },
            );
        }
    }
}

#[derive(Clone)]
enum DescriptorOpenGrant {
    ReadBlob {
        host_binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
        cancellation: Arc<DescriptorGrantCancellation>,
    },
    WriteBlob {
        operation: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
        access: AccessMode,
        cancellation: Arc<DescriptorGrantCancellation>,
    },
    Stream {
        path: DataPath,
        operation: ActorAddress,
        host_binding: ActorAddress,
        incarnation: StreamIncarnation,
        ring: RingHandle,
        role: Role,
        cancellation: Arc<DescriptorGrantCancellation>,
    },
}

#[derive(Clone, Copy)]
enum DescriptorGrantCancellationAction {
    HostOpen {
        host_session: ActorAddress,
        operation: ActorAddress,
    },
    WriteBlob {
        operation: ActorAddress,
        lease: BlobLease,
    },
}

struct DescriptorGrantCancellation {
    runtime: Runtime,
    action: DescriptorGrantCancellationAction,
    armed: AtomicBool,
}

impl DescriptorGrantCancellation {
    fn disarm(&self) {
        self.armed.store(false, Ordering::Release);
    }
}

impl Drop for DescriptorGrantCancellation {
    fn drop(&mut self) {
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }
        match self.action {
            DescriptorGrantCancellationAction::HostOpen {
                host_session,
                operation,
            } => {
                let _ = self
                    .runtime
                    .send_to(host_session, HostSessionIn::CancelOpen { operation });
            }
            DescriptorGrantCancellationAction::WriteBlob { operation, lease } => {
                let _ = self.runtime.send_to(
                    operation,
                    ChildOperationIn::AbortRequested {
                        reply_to: None,
                        lease,
                    },
                );
            }
        }
    }
}

#[derive(Clone)]
pub struct DataPlane {
    runtime: Runtime,
    child_session: ActorAddress,
    arena: Arc<MappedArena>,
    transitions: Option<TransitionEmitter>,
    _route_watch: Option<Arc<HostSessionRouteWatch>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protection {
    Read,
    ReadWrite,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sharing {
    Shared,
    Private,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MapTarget {
    Host,
    Device(DeviceId),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MapRequest {
    pub protection: Protection,
    pub sharing: Sharing,
    pub target: MapTarget,
    pub offset: u64,
    pub length: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RegionKind {
    Host,
    Arena,
    Device,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferRoute {
    Direct,
    Staged,
}

pub struct DeviceRegion {
    identity: [u8; 32],
    generation: u64,
    length: u64,
    readable: bool,
    writable: bool,
    direct_required: bool,
}

impl DeviceRegion {
    pub const fn identity(&self) -> &[u8; 32] {
        &self.identity
    }

    pub const fn generation(&self) -> u64 {
        self.generation
    }

    pub const fn length(&self) -> u64 {
        self.length
    }

    pub const fn is_readable(&self) -> bool {
        self.readable
    }

    pub const fn is_writable(&self) -> bool {
        self.writable
    }

    pub const fn requires_direct_route(&self) -> bool {
        self.direct_required
    }
}

pub struct DeviceRegionSlice<'a> {
    region: &'a DeviceRegion,
    offset: u64,
    length: u64,
}

pub enum RegionSlice<'a> {
    Host(&'a mut [u8]),
    Arena(&'a mut [u8]),
    Device(DeviceRegionSlice<'a>),
}

impl<'a> RegionSlice<'a> {
    pub fn host(bytes: &'a mut [u8]) -> Self {
        Self::Host(bytes)
    }

    pub fn arena(bytes: &'a mut [u8]) -> Self {
        Self::Arena(bytes)
    }

    pub fn device(
        region: &'a DeviceRegion,
        offset: u64,
        length: u64,
    ) -> Result<Self, DataPlaneError> {
        if offset
            .checked_add(length)
            .is_none_or(|end| end > region.length)
        {
            return Err(DataPlaneError::InvalidArgument(
                "device region slice is out of bounds".to_owned(),
            ));
        }
        Ok(Self::Device(DeviceRegionSlice {
            region,
            offset,
            length,
        }))
    }

    pub const fn kind(&self) -> RegionKind {
        match self {
            Self::Host(_) => RegionKind::Host,
            Self::Arena(_) => RegionKind::Arena,
            Self::Device(_) => RegionKind::Device,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Host(bytes) | Self::Arena(bytes) => bytes.len(),
            Self::Device(slice) => usize::try_from(slice.length).unwrap_or(usize::MAX),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn as_ref(&self) -> Result<&[u8], DataPlaneError> {
        match self {
            Self::Host(bytes) | Self::Arena(bytes) => Ok(bytes),
            Self::Device(slice) if !slice.region.readable => Err(DataPlaneError::BadDescriptor),
            Self::Device(slice) => {
                let _ = slice.offset;
                Err(DataPlaneError::Unsupported(
                    "no registered device read route is installed".to_owned(),
                ))
            }
        }
    }

    fn as_mut(&mut self) -> Result<&mut [u8], DataPlaneError> {
        match self {
            Self::Host(bytes) | Self::Arena(bytes) => Ok(bytes),
            Self::Device(slice) if !slice.region.writable => Err(DataPlaneError::BadDescriptor),
            Self::Device(slice) => {
                let _ = slice.offset;
                Err(DataPlaneError::Unsupported(
                    "no registered device write route is installed".to_owned(),
                ))
            }
        }
    }
}

pub enum DescriptorMapping {
    ReadOnly(BlobView),
    WritableReadOnly(WritableArenaView),
    Writable(WritableArenaView),
}

impl DescriptorMapping {
    pub fn len(&self) -> usize {
        match self {
            Self::ReadOnly(view) => view.len(),
            Self::WritableReadOnly(view) | Self::Writable(view) => view.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn as_mut(&mut self) -> Result<&mut [u8], DataPlaneError> {
        match self {
            Self::ReadOnly(_) | Self::WritableReadOnly(_) => Err(DataPlaneError::BadDescriptor),
            Self::Writable(view) => Ok(view.as_mut()),
        }
    }

    pub const fn route(&self) -> TransferRoute {
        TransferRoute::Direct
    }
}

impl AsRef<[u8]> for DescriptorMapping {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::ReadOnly(view) => view.as_ref(),
            Self::WritableReadOnly(view) | Self::Writable(view) => view.as_ref(),
        }
    }
}

impl fmt::Debug for DescriptorMapping {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DescriptorMapping")
            .field("len", &self.len())
            .field("route", &self.route())
            .field("writable", &matches!(self, Self::Writable(_)))
            .finish()
    }
}

enum DescriptorBackend {
    ReadBlob { blob: Blob, offset: u64 },
    WriteBlob { writer: BlobWriter, offset: u64 },
    ReadStream(StreamReader),
    WriteStream(StreamWriter),
}

pub struct Descriptor {
    kind: DescriptorKind,
    capabilities: DescriptorCapabilities,
    access: AccessMode,
    backend: Option<DescriptorBackend>,
    last_route: Option<TransferRoute>,
}

impl Descriptor {
    fn read_blob(blob: Blob) -> Self {
        Self {
            kind: DescriptorKind::Blob,
            capabilities: DescriptorCapabilities::READ
                .union(DescriptorCapabilities::MAP_HOST)
                .union(DescriptorCapabilities::SEEK),
            access: AccessMode::ReadOnly,
            backend: Some(DescriptorBackend::ReadBlob { blob, offset: 0 }),
            last_route: None,
        }
    }

    fn write_blob(writer: BlobWriter, access: AccessMode) -> Self {
        let mut capabilities = DescriptorCapabilities::WRITE
            .union(DescriptorCapabilities::MAP_HOST)
            .union(DescriptorCapabilities::SEEK);
        if access.can_read() {
            capabilities = capabilities.union(DescriptorCapabilities::READ);
        }
        Self {
            kind: DescriptorKind::Blob,
            capabilities,
            access,
            backend: Some(DescriptorBackend::WriteBlob { writer, offset: 0 }),
            last_route: None,
        }
    }

    fn read_stream(reader: StreamReader) -> Self {
        Self {
            kind: DescriptorKind::Stream,
            capabilities: DescriptorCapabilities::READ,
            access: AccessMode::ReadOnly,
            backend: Some(DescriptorBackend::ReadStream(reader)),
            last_route: None,
        }
    }

    fn write_stream(writer: StreamWriter) -> Self {
        Self {
            kind: DescriptorKind::Stream,
            capabilities: DescriptorCapabilities::WRITE,
            access: AccessMode::WriteOnly,
            backend: Some(DescriptorBackend::WriteStream(writer)),
            last_route: None,
        }
    }

    pub const fn kind(&self) -> DescriptorKind {
        self.kind
    }

    pub const fn capabilities(&self) -> DescriptorCapabilities {
        self.capabilities
    }

    pub const fn access(&self) -> AccessMode {
        self.access
    }

    pub const fn last_route(&self) -> Option<TransferRoute> {
        self.last_route
    }

    pub fn is_closed(&self) -> bool {
        self.backend.is_none()
    }

    pub async fn read(&mut self, destination: &mut [u8]) -> Result<usize, DataPlaneError> {
        if !self.access.can_read() {
            return Err(DataPlaneError::BadDescriptor);
        }
        let backend = self.backend.as_mut().ok_or(DataPlaneError::BadDescriptor)?;
        if destination.is_empty() {
            return Ok(0);
        }
        let result = match backend {
            DescriptorBackend::ReadBlob { blob, offset } => {
                let count = blob.copy_at(*offset, destination)?;
                *offset += count as u64;
                Ok(count)
            }
            DescriptorBackend::WriteBlob { writer, offset } => {
                let count = writer.copy_at(*offset, destination)?;
                *offset += count as u64;
                Ok(count)
            }
            DescriptorBackend::ReadStream(reader) => reader.read_into(destination).await,
            DescriptorBackend::WriteStream(_) => Err(DataPlaneError::BadDescriptor),
        };
        if result.is_ok() {
            self.last_route = Some(TransferRoute::Staged);
        }
        result
    }

    pub async fn write(&mut self, source: &[u8]) -> Result<usize, DataPlaneError> {
        if !self.access.can_write() {
            return Err(DataPlaneError::BadDescriptor);
        }
        let backend = self.backend.as_mut().ok_or(DataPlaneError::BadDescriptor)?;
        if source.is_empty() {
            return Ok(0);
        }
        let result = match backend {
            DescriptorBackend::WriteBlob { writer, offset } => {
                let count = writer.copy_from(*offset, source)?;
                *offset += count as u64;
                Ok(count)
            }
            DescriptorBackend::WriteStream(writer) => writer.write_partial(source).await,
            DescriptorBackend::ReadBlob { .. } | DescriptorBackend::ReadStream(_) => {
                Err(DataPlaneError::BadDescriptor)
            }
        };
        if result.is_ok() {
            self.last_route = Some(TransferRoute::Staged);
        }
        result
    }

    pub async fn read_into(
        &mut self,
        mut destination: RegionSlice<'_>,
    ) -> Result<usize, DataPlaneError> {
        let destination = destination.as_mut()?;
        self.read(destination).await
    }

    pub async fn write_from(&mut self, source: RegionSlice<'_>) -> Result<usize, DataPlaneError> {
        let source = source.as_ref()?;
        self.write(source).await
    }

    pub async fn read_exact(&mut self, destination: &mut [u8]) -> Result<(), DataPlaneError> {
        let mut completed = 0;
        while completed < destination.len() {
            let count = self.read(&mut destination[completed..]).await?;
            if count == 0 {
                return Err(DataPlaneError::InvalidArgument(
                    "unexpected EOF during read_exact".to_owned(),
                ));
            }
            completed += count;
        }
        Ok(())
    }

    pub async fn write_all(&mut self, source: &[u8]) -> Result<(), DataPlaneError> {
        let mut completed = 0;
        while completed < source.len() {
            let count = self.write(&source[completed..]).await?;
            if count == 0 {
                return Err(DataPlaneError::SessionFailed(
                    "zero-byte write made no progress".to_owned(),
                ));
            }
            completed += count;
        }
        Ok(())
    }

    pub fn map(&self, request: MapRequest) -> Result<DescriptorMapping, DataPlaneError> {
        let backend = self.backend.as_ref().ok_or(DataPlaneError::BadDescriptor)?;
        if request.target != MapTarget::Host || request.sharing != Sharing::Shared {
            return Err(DataPlaneError::Unsupported(
                "requested mapping target or sharing mode is not supported".to_owned(),
            ));
        }
        match backend {
            DescriptorBackend::ReadBlob { blob, .. } => {
                if request.protection != Protection::Read {
                    return Err(DataPlaneError::BadDescriptor);
                }
                Ok(DescriptorMapping::ReadOnly(
                    blob.map_range(request.offset, request.length)?,
                ))
            }
            DescriptorBackend::WriteBlob { writer, .. } => match request.protection {
                Protection::Read if self.access.can_read() => {
                    Ok(DescriptorMapping::WritableReadOnly(
                        writer.map_range(request.offset, request.length)?,
                    ))
                }
                Protection::Read => Err(DataPlaneError::BadDescriptor),
                Protection::ReadWrite if self.access.can_write() => Ok(
                    DescriptorMapping::Writable(writer.map_range(request.offset, request.length)?),
                ),
                Protection::ReadWrite => Err(DataPlaneError::BadDescriptor),
            },
            DescriptorBackend::ReadStream(_) | DescriptorBackend::WriteStream(_) => {
                Err(DataPlaneError::MappingUnsupported)
            }
        }
    }

    pub async fn close(&mut self) -> Result<(), DataPlaneError> {
        let backend = self.backend.take().ok_or(DataPlaneError::BadDescriptor)?;
        match backend {
            DescriptorBackend::ReadBlob { .. } => Ok(()),
            DescriptorBackend::WriteBlob { mut writer, .. } => {
                if writer.defer_seal_if_mapped() {
                    Ok(())
                } else {
                    writer.seal().await
                }
            }
            DescriptorBackend::ReadStream(mut reader) => reader.close_descriptor().await,
            DescriptorBackend::WriteStream(mut writer) => writer.close().await,
        }
    }

    pub async fn abort(&mut self) -> Result<(), DataPlaneError> {
        let backend = self.backend.take().ok_or(DataPlaneError::BadDescriptor)?;
        match backend {
            DescriptorBackend::ReadBlob { .. } => Ok(()),
            DescriptorBackend::WriteBlob { mut writer, .. } => {
                if writer.defer_abort_if_mapped() {
                    Ok(())
                } else {
                    writer.abort().await
                }
            }
            DescriptorBackend::ReadStream(mut reader) => reader.abort_descriptor().await,
            DescriptorBackend::WriteStream(mut writer) => writer.abort_descriptor().await,
        }
    }

    fn into_blob(mut self) -> Result<Blob, DataPlaneError> {
        match self.backend.take() {
            Some(DescriptorBackend::ReadBlob { blob, .. }) => Ok(blob),
            _ => Err(DataPlaneError::BadDescriptor),
        }
    }

    fn into_blob_writer(mut self) -> Result<BlobWriter, DataPlaneError> {
        match self.backend.take() {
            Some(DescriptorBackend::WriteBlob { writer, .. }) => Ok(writer),
            _ => Err(DataPlaneError::BadDescriptor),
        }
    }

    fn into_stream_reader(mut self) -> Result<StreamReader, DataPlaneError> {
        match self.backend.take() {
            Some(DescriptorBackend::ReadStream(reader)) => Ok(reader),
            _ => Err(DataPlaneError::BadDescriptor),
        }
    }

    fn into_stream_writer(mut self) -> Result<StreamWriter, DataPlaneError> {
        match self.backend.take() {
            Some(DescriptorBackend::WriteStream(writer)) => Ok(writer),
            _ => Err(DataPlaneError::BadDescriptor),
        }
    }
}

impl Drop for Descriptor {
    fn drop(&mut self) {
        let Some(backend) = self.backend.take() else {
            return;
        };
        drop(backend);
    }
}

impl fmt::Debug for Descriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Descriptor")
            .field("kind", &self.kind)
            .field("capabilities", &self.capabilities)
            .field("access", &self.access)
            .field("closed", &self.is_closed())
            .finish()
    }
}

impl DataPlane {
    pub fn child_session(&self) -> ActorAddress {
        self.child_session
    }

    pub fn arena(&self) -> &Arc<MappedArena> {
        &self.arena
    }

    async fn namespace_operation(
        &self,
        request: NamespaceOperation,
    ) -> Result<NamespaceOperationResult, DataPlaneError> {
        let inbox = self
            .runtime
            .new_inbox::<Result<NamespaceOperationResult, DataPlaneError>>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let reply_to = *inbox.addr();
        self.runtime
            .send_to(
                self.child_session,
                ChildSessionIn::Namespace { request, reply_to },
            )
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let mut cancellation = NamespaceOperationCancellation {
            runtime: self.runtime.clone(),
            child_session: self.child_session,
            reply_to,
            armed: true,
        };
        let result = inbox.recv().await;
        cancellation.armed = false;
        result
    }

    pub async fn lookup(
        &self,
        path: &DataPath,
    ) -> Result<crate::namespace::NamespaceNode, DataPlaneError> {
        match self
            .namespace_operation(NamespaceOperation::Lookup { path: path.clone() })
            .await?
        {
            NamespaceOperationResult::Node(node) => Ok(node),
            NamespaceOperationResult::Mutation { .. } => Err(DataPlaneError::SessionFailed(
                "namespace lookup returned a mutation receipt".to_owned(),
            )),
        }
    }

    pub async fn unlink(&self, path: &DataPath) -> Result<u64, DataPlaneError> {
        match self
            .namespace_operation(NamespaceOperation::Unlink { path: path.clone() })
            .await?
        {
            NamespaceOperationResult::Mutation { revision } => Ok(revision),
            NamespaceOperationResult::Node(_) => Err(DataPlaneError::SessionFailed(
                "namespace unlink returned a lookup result".to_owned(),
            )),
        }
    }

    pub async fn rename(
        &self,
        source: &DataPath,
        destination: &DataPath,
        replace: bool,
    ) -> Result<u64, DataPlaneError> {
        match self
            .namespace_operation(NamespaceOperation::Rename {
                source: source.clone(),
                destination: destination.clone(),
                replace,
            })
            .await?
        {
            NamespaceOperationResult::Mutation { revision } => Ok(revision),
            NamespaceOperationResult::Node(_) => Err(DataPlaneError::SessionFailed(
                "namespace rename returned a lookup result".to_owned(),
            )),
        }
    }

    fn descriptor_from_grant(
        &self,
        grant: DescriptorOpenGrant,
    ) -> Result<Descriptor, DataPlaneError> {
        match grant {
            DescriptorOpenGrant::ReadBlob {
                host_binding,
                lease,
                metadata,
                cancellation,
            } => {
                let releaser: Arc<dyn LeaseReleaser> = Arc::new(RuntimeLeaseReleaser {
                    runtime: self.runtime.clone(),
                    child_session: self.child_session,
                    host_binding,
                });
                let blob = Blob::from_sealed_lease(self.arena.clone(), lease, metadata, releaser)?;
                cancellation.disarm();
                Ok(Descriptor::read_blob(blob))
            }
            DescriptorOpenGrant::WriteBlob {
                operation,
                lease,
                metadata,
                access,
                cancellation,
            } => {
                let writable = WritableBlobLease::from_grant(self.arena.clone(), lease, metadata)?;
                let lifecycle = Arc::new(WritableDescriptorLifecycle {
                    runtime: self.runtime.clone(),
                    operation,
                    writable: Arc::downgrade(&writable),
                    terminal: AtomicU8::new(0),
                    completed: AtomicBool::new(false),
                });
                let observer: Arc<dyn WritableViewObserver> = lifecycle.clone();
                writable.set_view_observer(observer);
                let writer = BlobWriter {
                    runtime: self.runtime.clone(),
                    operation,
                    writable,
                    lifecycle,
                    finalized: false,
                };
                cancellation.disarm();
                Ok(Descriptor::write_blob(writer, access))
            }
            DescriptorOpenGrant::Stream {
                path,
                operation,
                host_binding,
                incarnation,
                ring,
                role,
                cancellation,
            } => {
                let endpoint = attach_mapped(&self.arena, ring, role).map_err(|error| {
                    DataPlaneError::StreamFault(format!("attach descriptor stream: {error:?}"))
                })?;
                let descriptor = match role {
                    Role::Consumer => Descriptor::read_stream(StreamReader {
                        runtime: self.runtime.clone(),
                        child_session: self.child_session,
                        operation,
                        host_binding,
                        incarnation,
                        endpoint,
                        terminal: None,
                        pending_record: None,
                        path,
                    }),
                    Role::Producer => Descriptor::write_stream(StreamWriter {
                        runtime: self.runtime.clone(),
                        child_session: self.child_session,
                        path,
                        operation,
                        host_binding,
                        incarnation,
                        endpoint,
                        closed: false,
                    }),
                };
                cancellation.disarm();
                Ok(descriptor)
            }
        }
    }

    async fn open_inner(
        &self,
        path: &DataPath,
        options: OpenOptions,
        policy: OpenPolicy,
    ) -> Result<Descriptor, DataPlaneError> {
        options.validate()?;
        let inbox = self
            .runtime
            .new_inbox::<Result<DescriptorOpenGrant, DataPlaneError>>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let reply_to = *inbox.addr();
        self.runtime
            .send_to(
                self.child_session,
                ChildSessionIn::Open {
                    path: path.clone(),
                    options,
                    policy,
                    reply_to,
                },
            )
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let mut cancellation = DescriptorOpenCancellation {
            runtime: self.runtime.clone(),
            child_session: self.child_session,
            reply_to,
            armed: true,
        };
        let result = inbox.recv().await;
        cancellation.armed = false;
        self.descriptor_from_grant(result?)
    }

    pub async fn open(
        &self,
        path: &DataPath,
        options: OpenOptions,
    ) -> Result<Descriptor, DataPlaneError> {
        self.open_inner(path, options, OpenPolicy::Ordinary).await
    }

    pub async fn open_path(
        &self,
        path: &str,
        options: OpenOptions,
    ) -> Result<Descriptor, DataPlaneError> {
        let path = DataPath::parse(path)
            .map_err(|error| DataPlaneError::InvalidPath(error.to_string()))?;
        self.open(&path, options).await
    }

    pub async fn read_blob(&self, path: &DataPath) -> Result<Blob, DataPlaneError> {
        self.open(path, OpenOptions::read_only()).await?.into_blob()
    }

    pub async fn read_blob_path(&self, path: &str) -> Result<Blob, DataPlaneError> {
        let path = DataPath::parse(path)
            .map_err(|error| DataPlaneError::InvalidPath(error.to_string()))?;
        self.read_blob(&path).await
    }

    pub async fn write_blob(
        &self,
        path: &DataPath,
        length: u64,
    ) -> Result<BlobWriter, DataPlaneError> {
        self.open(path, OpenOptions::staged_blob(length))
            .await?
            .into_blob_writer()
    }

    pub async fn write_blob_path(
        &self,
        path: &str,
        length: u64,
    ) -> Result<BlobWriter, DataPlaneError> {
        let path = DataPath::parse(path)
            .map_err(|error| DataPlaneError::InvalidPath(error.to_string()))?;
        self.write_blob(&path, length).await
    }

    pub async fn read_stream(&self, path: &DataPath) -> Result<StreamReader, DataPlaneError> {
        self.open_inner(
            path,
            OpenOptions {
                access: AccessMode::ReadOnly,
                ..OpenOptions::default()
            },
            OpenPolicy::EnsureStream { replace: false },
        )
        .await?
        .into_stream_reader()
    }

    pub async fn write_stream(&self, path: &DataPath) -> Result<StreamWriter, DataPlaneError> {
        self.open_inner(
            path,
            OpenOptions {
                access: AccessMode::WriteOnly,
                ..OpenOptions::default()
            },
            OpenPolicy::EnsureStream { replace: false },
        )
        .await?
        .into_stream_writer()
    }

    pub async fn write_stream_replacing(
        &self,
        path: &DataPath,
    ) -> Result<StreamWriter, DataPlaneError> {
        self.open_inner(
            path,
            OpenOptions {
                access: AccessMode::WriteOnly,
                ..OpenOptions::default()
            },
            OpenPolicy::EnsureStream { replace: true },
        )
        .await?
        .into_stream_writer()
    }

    pub fn close(&self) -> Result<(), DataPlaneError> {
        self.runtime
            .send_to(self.child_session, ChildSessionIn::Close { reply_to: None })
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))
    }

    /// Closes the child and host session and waits for the host to finish
    /// revoking its resources. This future has no internal timeout; callers
    /// that require a bound must apply one.
    pub async fn close_acknowledged(&self) -> Result<(), DataPlaneError> {
        let inbox = self
            .runtime
            .new_inbox::<Result<(), DataPlaneError>>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        self.runtime
            .send_to(
                self.child_session,
                ChildSessionIn::Close {
                    reply_to: Some(*inbox.addr()),
                },
            )
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        inbox.recv().await
    }
}

pub trait StreamConsumer: Send + Sync + 'static {
    fn consume(&self, bytes: &[u8]) -> Result<(), String>;
}

impl DataPlane {
    pub fn collect_stream(
        &self,
        path: DataPath,
        consumer: Arc<dyn StreamConsumer>,
    ) -> Result<ActorCompletion<Result<(), DataPlaneError>>, DataPlaneError> {
        let completion = ActorCompletion::new();
        self.runtime
            .spawn(StreamConsumerActor {
                child_session: self.child_session,
                arena: self.arena.clone(),
                path,
                consumer,
                completion: completion.clone(),
                operation: None,
                incarnation: None,
                host_binding: None,
                endpoint: None,
                pending_result: None,
                close_operation: None,
                phase: ConsumerPhase::Opening,
                transitions: self.transitions.clone(),
            })
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        Ok(completion)
    }
}
struct WritableDescriptorLifecycle {
    runtime: Runtime,
    operation: ActorAddress,
    writable: Weak<WritableBlobLease>,
    terminal: AtomicU8,
    completed: AtomicBool,
}

impl WritableDescriptorLifecycle {
    const CLOSE: u8 = 1;
    const ABORT: u8 = 2;

    fn request(&self, terminal: u8) {
        let _ = self
            .terminal
            .compare_exchange(0, terminal, Ordering::AcqRel, Ordering::Acquire);
        if self
            .writable
            .upgrade()
            .is_some_and(|writable| !writable.has_active_view())
        {
            self.finalize();
        }
    }

    fn finalize(&self) {
        if self.completed.swap(true, Ordering::AcqRel) {
            return;
        }
        let Some(writable) = self.writable.upgrade() else {
            return;
        };
        let lease = writable.lease();
        match self.terminal.load(Ordering::Acquire) {
            Self::CLOSE => match writable.seal() {
                Ok(metadata) => {
                    let _ = self.runtime.send_to(
                        self.operation,
                        ChildOperationIn::SealRequested {
                            reply_to: None,
                            lease,
                            metadata,
                        },
                    );
                }
                Err(_) => {
                    let _ = self.runtime.send_to(
                        self.operation,
                        ChildOperationIn::AbortRequested {
                            reply_to: None,
                            lease,
                        },
                    );
                }
            },
            Self::ABORT => {
                let _ = writable.abort();
                let _ = self.runtime.send_to(
                    self.operation,
                    ChildOperationIn::AbortRequested {
                        reply_to: None,
                        lease,
                    },
                );
            }
            _ => {
                self.completed.store(false, Ordering::Release);
            }
        }
    }
}

impl WritableViewObserver for WritableDescriptorLifecycle {
    fn view_released(&self) {
        if self.terminal.load(Ordering::Acquire) != 0 {
            self.finalize();
        }
    }
}

pub struct BlobWriter {
    runtime: Runtime,
    operation: ActorAddress,
    writable: Arc<WritableBlobLease>,
    lifecycle: Arc<WritableDescriptorLifecycle>,
    finalized: bool,
}

impl BlobWriter {
    pub fn length(&self) -> u64 {
        self.writable.metadata().length
    }

    pub fn map(&self) -> Result<WritableArenaView, DataPlaneError> {
        self.writable.map().map_err(Into::into)
    }

    fn copy_at(&self, offset: u64, destination: &mut [u8]) -> Result<usize, DataPlaneError> {
        self.writable
            .copy_at(offset, destination)
            .map_err(Into::into)
    }

    fn copy_from(&self, offset: u64, source: &[u8]) -> Result<usize, DataPlaneError> {
        let end = offset.checked_add(source.len() as u64).ok_or_else(|| {
            DataPlaneError::Unsupported("blob growth is not supported".to_owned())
        })?;
        if end > self.length() {
            return Err(DataPlaneError::Unsupported(
                "blob growth is not supported".to_owned(),
            ));
        }
        self.writable.copy_from(offset, source).map_err(Into::into)
    }

    fn map_range(&self, offset: u64, length: u64) -> Result<WritableArenaView, DataPlaneError> {
        self.writable.map_range(offset, length).map_err(Into::into)
    }

    pub async fn seal(&mut self) -> Result<(), DataPlaneError> {
        let metadata = self.writable.seal()?;
        let lease = self.writable.lease();
        let ask = self
            .runtime
            .ask::<ChildOperationIn, Result<(), DataPlaneError>>(self.operation, |reply_to| {
                ChildOperationIn::SealRequested {
                    reply_to: Some(reply_to),
                    lease,
                    metadata,
                }
            })
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let result = ask.await;
        if result.is_ok() {
            self.finalized = true;
        }
        result
    }

    pub async fn abort(&mut self) -> Result<(), DataPlaneError> {
        self.writable.abort()?;
        self.finalized = true;
        let lease = self.writable.lease();
        let ask = self
            .runtime
            .ask::<ChildOperationIn, Result<(), DataPlaneError>>(self.operation, |reply_to| {
                ChildOperationIn::AbortRequested {
                    reply_to: Some(reply_to),
                    lease,
                }
            })
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        ask.await
    }

    fn defer_seal_if_mapped(&mut self) -> bool {
        if !self.writable.has_active_view() {
            return false;
        }
        self.finalized = true;
        self.lifecycle.request(WritableDescriptorLifecycle::CLOSE);
        true
    }

    fn defer_abort_if_mapped(&mut self) -> bool {
        if !self.writable.has_active_view() {
            return false;
        }
        self.finalized = true;
        self.lifecycle.request(WritableDescriptorLifecycle::ABORT);
        true
    }
}

impl Drop for BlobWriter {
    fn drop(&mut self) {
        if self.finalized {
            return;
        }
        let should_abort = self.writable.is_finished() || self.writable.abort().is_ok();
        if should_abort {
            let _ = self.runtime.send_to(
                self.operation,
                ChildOperationIn::AbortRequested {
                    reply_to: None,
                    lease: self.writable.lease(),
                },
            );
        }
    }
}

#[derive(Clone)]
pub(crate) struct StreamOpenGrant {
    operation: ActorAddress,
    host_binding: ActorAddress,
    ring: RingHandle,
    incarnation: StreamIncarnation,
    cancellation: Arc<DescriptorGrantCancellation>,
}

#[derive(Clone)]
pub(crate) enum ChildStreamIn {
    Opened(Result<StreamOpenGrant, DataPlaneError>),
    Wake(Result<(), DataPlaneError>),
    CloseCompleted(Result<(), DataPlaneError>),
}

fn stream_flow_error(path: &DataPath, action: &str, error: FlowError) -> DataPlaneError {
    match error {
        FlowError::Corrupt(HeaderError::GenerationMismatch { .. }) => {
            DataPlaneError::PathReplaced(path.clone())
        }
        error => DataPlaneError::StreamFault(format!("{action}: {error:?}")),
    }
}

pub struct StreamWriter {
    runtime: Runtime,
    child_session: ActorAddress,
    operation: ActorAddress,
    path: DataPath,
    host_binding: ActorAddress,
    incarnation: StreamIncarnation,
    endpoint: Endpoint,
    closed: bool,
}

impl StreamWriter {
    /// The authoritative namespace identity of this attached stream.
    ///
    /// This identity remains unchanged after close or namespace replacement.
    pub fn incarnation(&self) -> StreamIncarnation {
        self.incarnation
    }

    pub fn capacity(&self) -> u64 {
        self.endpoint.capacity()
    }
    fn send_control(&self, message: HostStreamIn) -> Result<(), DataPlaneError> {
        self.runtime
            .send_to(
                self.child_session,
                ChildSessionIn::StreamControl {
                    binding: self.host_binding,
                    message,
                },
            )
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))
    }

    async fn close_host(&mut self) -> Result<(), DataPlaneError> {
        let inbox = self
            .runtime
            .new_inbox::<ChildStreamIn>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        self.send_control(HostStreamIn::Close {
            clean: true,
            reply_to: Some(*inbox.addr()),
        })?;
        match inbox.recv().await {
            ChildStreamIn::Wake(result) => result,
            ChildStreamIn::Opened(_) | ChildStreamIn::CloseCompleted(_) => {
                Err(DataPlaneError::StreamFault(
                    "received stream-open result while closing writer".to_owned(),
                ))
            }
        }
    }

    async fn send_one(&mut self, kind: RecordKind, bytes: &[u8]) -> Result<(), DataPlaneError> {
        loop {
            match self.endpoint.send_record(kind, bytes) {
                Ok(()) => {
                    self.send_control(HostStreamIn::DataAvailable)?;
                    return Ok(());
                }
                Err(FlowError::InsufficientSpace { .. }) => {
                    let inbox = self
                        .runtime
                        .new_inbox::<ChildStreamIn>()
                        .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
                    self.send_control(HostStreamIn::WaitCapacity {
                        reply_to: *inbox.addr(),
                    })?;
                    match self.endpoint.send_record(kind, bytes) {
                        Ok(()) => {
                            self.send_control(HostStreamIn::DataAvailable)?;
                            return Ok(());
                        }
                        Err(FlowError::InsufficientSpace { .. }) => match inbox.recv().await {
                            ChildStreamIn::Wake(result) => result?,
                            ChildStreamIn::Opened(_) | ChildStreamIn::CloseCompleted(_) => {
                                return Err(DataPlaneError::StreamFault(
                                    "received stream-open result while waiting for capacity"
                                        .to_owned(),
                                ));
                            }
                        },
                        Err(error) => {
                            return Err(DataPlaneError::StreamFault(format!(
                                "write stream ring: {error:?}"
                            )));
                        }
                    }
                }
                Err(error) => {
                    return Err(DataPlaneError::StreamFault(format!(
                        "write stream ring: {error:?}"
                    )));
                }
            }
        }
    }

    pub async fn flush(&mut self) -> Result<(), DataPlaneError> {
        let target = self
            .endpoint
            .positions()
            .map_err(|error| {
                DataPlaneError::StreamFault(format!("observe stream flush position: {error:?}"))
            })?
            .0;
        loop {
            let consumed = self
                .endpoint
                .positions()
                .map_err(|error| {
                    DataPlaneError::StreamFault(format!("observe stream flush progress: {error:?}"))
                })?
                .1;
            if consumed >= target {
                return Ok(());
            }
            let inbox = self
                .runtime
                .new_inbox::<ChildStreamIn>()
                .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
            // The host checks the shared consumed cursor when registering this
            // target, closing the race with an already-delivered progress edge.
            self.send_control(HostStreamIn::WaitProgress {
                reply_to: *inbox.addr(),
                target,
            })?;
            self.send_control(HostStreamIn::DataAvailable)?;
            if self
                .endpoint
                .positions()
                .map_err(|error| {
                    DataPlaneError::StreamFault(format!("observe stream flush progress: {error:?}"))
                })?
                .1
                >= target
            {
                return Ok(());
            }
            match inbox.recv().await {
                ChildStreamIn::Wake(result) => {
                    if let Err(error) = result
                        && self
                            .endpoint
                            .positions()
                            .map_err(|flow| {
                                DataPlaneError::StreamFault(format!(
                                    "observe terminal flush progress: {flow:?}"
                                ))
                            })?
                            .1
                            < target
                    {
                        return Err(error);
                    }
                }
                ChildStreamIn::Opened(_) | ChildStreamIn::CloseCompleted(_) => {
                    return Err(DataPlaneError::StreamFault(
                        "received stream-open result while flushing".to_owned(),
                    ));
                }
            }
        }
    }

    pub async fn write_partial(&mut self, bytes: &[u8]) -> Result<usize, DataPlaneError> {
        if self.closed {
            return Err(DataPlaneError::StreamClosed);
        }
        if bytes.is_empty() {
            return Ok(0);
        }
        loop {
            match self.endpoint.peer_termination().map_err(|error| {
                stream_flow_error(&self.path, "observe stream peer terminal state", error)
            })? {
                None => {}
                Some(PeerTermination::Closed) => return Err(DataPlaneError::BrokenPipe),
                Some(PeerTermination::PathReplaced) => {
                    return Err(DataPlaneError::PathReplaced(self.path.clone()));
                }
            }
            let available = self.endpoint.writable_payload_capacity().map_err(|error| {
                stream_flow_error(&self.path, "observe writable stream capacity", error)
            })?;
            if available != 0 {
                let count = bytes
                    .len()
                    .min(usize::try_from(available).unwrap_or(usize::MAX));
                let mut record = self
                    .endpoint
                    .reserve_record(RecordKind::Data, count as u64)
                    .map_err(|error| {
                        stream_flow_error(&self.path, "reserve partial stream record", error)
                    })?;
                let (first, second) = record.spans_mut();
                first.copy_from_slice(&bytes[..first.len()]);
                second.copy_from_slice(&bytes[first.len()..count]);
                record.commit().map_err(|error| {
                    DataPlaneError::StreamFault(format!("commit partial stream record: {error:?}"))
                })?;
                self.send_control(HostStreamIn::DataAvailable)?;
                return Ok(count);
            }

            let inbox = self
                .runtime
                .new_inbox::<ChildStreamIn>()
                .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
            self.send_control(HostStreamIn::WaitCapacity {
                reply_to: *inbox.addr(),
            })?;
            if self.endpoint.writable_payload_capacity().map_err(|error| {
                stream_flow_error(&self.path, "recheck writable stream capacity", error)
            })? != 0
            {
                continue;
            }
            match inbox.recv().await {
                ChildStreamIn::Wake(Ok(())) => {}
                ChildStreamIn::Wake(Err(
                    DataPlaneError::StreamClosed | DataPlaneError::PeerLost,
                )) => return Err(DataPlaneError::BrokenPipe),
                ChildStreamIn::Wake(Err(error)) => return Err(error),
                ChildStreamIn::Opened(_) | ChildStreamIn::CloseCompleted(_) => {
                    return Err(DataPlaneError::StreamFault(
                        "received stream-open result while waiting for capacity".to_owned(),
                    ));
                }
            }
        }
    }

    pub async fn write(&mut self, bytes: &[u8]) -> Result<(), DataPlaneError> {
        let mut completed = 0;
        while completed < bytes.len() {
            completed += self.write_partial(&bytes[completed..]).await?;
        }
        Ok(())
    }

    pub async fn close(&mut self) -> Result<(), DataPlaneError> {
        if self.closed {
            return Ok(());
        }
        self.send_one(RecordKind::Eof, &[]).await?;
        self.flush().await?;
        self.close_host().await?;
        self.closed = true;
        let _ = self.runtime.send_to(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: self.operation,
            },
        );
        Ok(())
    }

    pub fn abort(&mut self) -> Result<(), DataPlaneError> {
        if self.closed {
            return Ok(());
        }
        self.closed = true;
        self.send_control(HostStreamIn::Close {
            clean: false,
            reply_to: None,
        })
    }

    async fn abort_descriptor(&mut self) -> Result<(), DataPlaneError> {
        if self.closed {
            return Ok(());
        }
        let inbox = self
            .runtime
            .new_inbox::<ChildStreamIn>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        self.closed = true;
        self.send_control(HostStreamIn::Close {
            clean: false,
            reply_to: Some(*inbox.addr()),
        })?;
        let result = match inbox.recv().await {
            ChildStreamIn::Wake(result) => result,
            ChildStreamIn::Opened(_) | ChildStreamIn::CloseCompleted(_) => {
                Err(DataPlaneError::StreamFault(
                    "received stream-open result while aborting writer".to_owned(),
                ))
            }
        };
        let _ = self.runtime.send_to(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: self.operation,
            },
        );
        result
    }
}

impl Drop for StreamWriter {
    fn drop(&mut self) {
        if !self.closed {
            let _ = self.send_control(HostStreamIn::Close {
                clean: false,
                reply_to: None,
            });
        }
        let _ = self.runtime.send_to(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: self.operation,
            },
        );
    }
}

#[derive(Clone)]
enum StreamReadTerminal {
    Eof,
    Error(DataPlaneError),
}

pub struct StreamReader {
    runtime: Runtime,
    child_session: ActorAddress,
    operation: ActorAddress,
    host_binding: ActorAddress,
    incarnation: StreamIncarnation,
    path: DataPath,
    endpoint: Endpoint,
    terminal: Option<StreamReadTerminal>,
    pending_record: Option<(RecordCursor, u64)>,
}

impl StreamReader {
    /// The authoritative namespace identity of this attached stream.
    ///
    /// This identity remains unchanged after EOF or namespace replacement.
    pub fn incarnation(&self) -> StreamIncarnation {
        self.incarnation
    }

    pub fn capacity(&self) -> u64 {
        self.endpoint.capacity()
    }

    fn send_control(&self, message: HostStreamIn) -> Result<(), DataPlaneError> {
        self.runtime
            .send_to(
                self.child_session,
                ChildSessionIn::StreamControl {
                    binding: self.host_binding,
                    message,
                },
            )
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))
    }
    async fn close_host(&mut self, clean: bool) -> Result<(), DataPlaneError> {
        let inbox = self
            .runtime
            .new_inbox::<ChildStreamIn>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        self.send_control(HostStreamIn::Close {
            clean,
            reply_to: Some(*inbox.addr()),
        })?;
        match inbox.recv().await {
            ChildStreamIn::Wake(result) => result,
            ChildStreamIn::Opened(_) | ChildStreamIn::CloseCompleted(_) => {
                Err(DataPlaneError::StreamFault(
                    "received stream-open result while closing reader".to_owned(),
                ))
            }
        }
    }

    fn terminal_read_count(&self) -> Option<Result<usize, DataPlaneError>> {
        self.terminal.as_ref().map(|terminal| match terminal {
            StreamReadTerminal::Eof => Ok(0),
            StreamReadTerminal::Error(error) => Err(error.clone()),
        })
    }

    fn release_record(&mut self, cursor: RecordCursor) -> Result<(), DataPlaneError> {
        self.endpoint
            .release_record_cursor(cursor)
            .map_err(|error| stream_flow_error(&self.path, "consume stream ring", error))?;
        self.send_control(HostStreamIn::CapacityAvailable)
    }

    pub async fn read_into(&mut self, destination: &mut [u8]) -> Result<usize, DataPlaneError> {
        if destination.is_empty() {
            return Ok(0);
        }
        if let Some(result) = self.terminal_read_count() {
            return result;
        }
        loop {
            if self.pending_record.is_none()
                && let Some(cursor) = self
                    .endpoint
                    .record_cursor()
                    .map_err(|error| stream_flow_error(&self.path, "read stream ring", error))?
            {
                match cursor.kind() {
                    RecordKind::Data if cursor.is_empty() => {
                        self.release_record(cursor)?;
                        continue;
                    }
                    RecordKind::Data => {
                        self.pending_record = Some((cursor, 0));
                    }
                    RecordKind::Eof => {
                        self.release_record(cursor)?;
                        self.terminal = Some(StreamReadTerminal::Eof);
                        let close_result = self.close_host(true).await;
                        let _ = self.runtime.send_to(
                            self.child_session,
                            ChildSessionIn::OperationDone {
                                operation: self.operation,
                            },
                        );
                        close_result?;
                        return Ok(0);
                    }
                    RecordKind::Fault => {
                        let mut bytes = vec![0_u8; cursor.len()];
                        let count = self
                            .endpoint
                            .copy_record_range(cursor, 0, &mut bytes)
                            .map_err(|error| {
                                DataPlaneError::StreamFault(format!(
                                    "read stream fault record: {error:?}"
                                ))
                            })?;
                        bytes.truncate(count);
                        self.release_record(cursor)?;
                        let error = DataPlaneError::StreamFault(
                            String::from_utf8_lossy(&bytes).into_owned(),
                        );
                        self.terminal = Some(StreamReadTerminal::Error(error.clone()));
                        let _ = self.send_control(HostStreamIn::Close {
                            clean: false,
                            reply_to: None,
                        });
                        return Err(error);
                    }
                }
            }

            if let Some((cursor, offset)) = self.pending_record {
                let count = self
                    .endpoint
                    .copy_record_range(cursor, offset, destination)
                    .map_err(|error| {
                        DataPlaneError::StreamFault(format!(
                            "copy partial stream record: {error:?}"
                        ))
                    })?;
                let next_offset = offset + count as u64;
                if next_offset == cursor.len() as u64 {
                    self.pending_record = None;
                    self.release_record(cursor)?;
                } else {
                    self.pending_record = Some((cursor, next_offset));
                }
                return Ok(count);
            }

            let peer_termination = self.endpoint.peer_termination().map_err(|error| {
                stream_flow_error(&self.path, "observe stream peer terminal state", error)
            })?;
            match peer_termination {
                None => {}
                Some(PeerTermination::Closed) => return Err(DataPlaneError::PeerLost),
                Some(PeerTermination::PathReplaced) => {
                    return Err(DataPlaneError::PathReplaced(self.path.clone()));
                }
            }

            let inbox = self
                .runtime
                .new_inbox::<ChildStreamIn>()
                .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
            self.send_control(HostStreamIn::WaitData {
                reply_to: *inbox.addr(),
            })?;
            if self
                .endpoint
                .record_cursor()
                .map_err(|error| stream_flow_error(&self.path, "recheck stream ring", error))?
                .is_some()
            {
                continue;
            }
            match inbox.recv().await {
                // A retired host binding can only report StreamClosed. Its
                // ring was fenced before retirement, so re-read the ring for
                // buffered records, EOF, or the precise peer-loss/replacement
                // state rather than exposing a scheduling-dependent error.
                ChildStreamIn::Wake(Ok(()) | Err(DataPlaneError::StreamClosed)) => {}
                ChildStreamIn::Wake(Err(error)) => {
                    self.terminal = Some(StreamReadTerminal::Error(error.clone()));
                    return Err(error);
                }
                ChildStreamIn::Opened(_) | ChildStreamIn::CloseCompleted(_) => {
                    return Err(DataPlaneError::StreamFault(
                        "received stream-open result while waiting for data".to_owned(),
                    ));
                }
            }
        }
    }

    pub async fn read(&mut self) -> Result<Option<Vec<u8>>, DataPlaneError> {
        if matches!(self.terminal, Some(StreamReadTerminal::Eof)) {
            return Ok(None);
        }
        let capacity = usize::try_from(self.capacity())
            .unwrap_or(usize::MAX)
            .max(1);
        let mut bytes = vec![0_u8; capacity];
        let count = self.read_into(&mut bytes).await?;
        if count == 0 {
            Ok(None)
        } else {
            bytes.truncate(count);
            Ok(Some(bytes))
        }
    }

    async fn close_descriptor(&mut self) -> Result<(), DataPlaneError> {
        if self.terminal.is_some() {
            return Ok(());
        }
        let result = self.close_host(false).await;
        self.terminal = Some(StreamReadTerminal::Eof);
        let _ = self.runtime.send_to(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: self.operation,
            },
        );
        result
    }

    async fn abort_descriptor(&mut self) -> Result<(), DataPlaneError> {
        if self.terminal.is_some() {
            return Ok(());
        }
        let inbox = self
            .runtime
            .new_inbox::<ChildStreamIn>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        self.terminal = Some(StreamReadTerminal::Error(
            DataPlaneError::OperationCancelled,
        ));
        self.send_control(HostStreamIn::Close {
            clean: false,
            reply_to: Some(*inbox.addr()),
        })?;
        let result = match inbox.recv().await {
            ChildStreamIn::Wake(result) => result,
            ChildStreamIn::Opened(_) | ChildStreamIn::CloseCompleted(_) => {
                Err(DataPlaneError::StreamFault(
                    "received stream-open result while aborting reader".to_owned(),
                ))
            }
        };
        let _ = self.runtime.send_to(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: self.operation,
            },
        );
        result
    }
}

impl Drop for StreamReader {
    fn drop(&mut self) {
        if self.terminal.is_none() {
            let _ = self.send_control(HostStreamIn::Close {
                clean: false,
                reply_to: None,
            });
        }
        let _ = self.runtime.send_to(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: self.operation,
            },
        );
    }
}

struct StreamConsumerActor {
    child_session: ActorAddress,
    arena: Arc<MappedArena>,
    path: DataPath,
    consumer: Arc<dyn StreamConsumer>,
    completion: ActorCompletion<Result<(), DataPlaneError>>,
    operation: Option<ActorAddress>,
    incarnation: Option<StreamIncarnation>,
    host_binding: Option<ActorAddress>,
    endpoint: Option<Endpoint>,
    pending_result: Option<Result<(), DataPlaneError>>,
    close_operation: Option<ActorAddress>,
    phase: ConsumerPhase,
    transitions: Option<TransitionEmitter>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConsumerPhase {
    Opening,
    Open,
    Closing,
    Finished,
}

enum ConsumerDrainStep {
    Empty,
    Data,
    Eof,
    Fault(String),
}

fn consume_next_record(
    endpoint: &mut Endpoint,
    consumer: &dyn StreamConsumer,
) -> Result<ConsumerDrainStep, DataPlaneError> {
    let Some(view) = endpoint
        .peek_record()
        .map_err(|error| DataPlaneError::StreamFault(format!("collect stream ring: {error:?}")))?
    else {
        return Ok(ConsumerDrainStep::Empty);
    };
    let kind = view.kind();
    let (first, second) = view.spans();
    let fault = (kind == RecordKind::Fault).then(|| {
        let mut reason = Vec::with_capacity(first.len() + second.len());
        reason.extend_from_slice(first);
        reason.extend_from_slice(second);
        String::from_utf8_lossy(&reason).into_owned()
    });
    if kind == RecordKind::Data {
        consumer
            .consume(first)
            .and_then(|()| consumer.consume(second))
            .map_err(DataPlaneError::StreamFault)?;
    }
    view.release().map_err(|error| {
        DataPlaneError::StreamFault(format!("release collected stream ring: {error:?}"))
    })?;
    Ok(match kind {
        RecordKind::Data => ConsumerDrainStep::Data,
        RecordKind::Eof => ConsumerDrainStep::Eof,
        RecordKind::Fault => ConsumerDrainStep::Fault(fault.expect("fault payload captured")),
    })
}

impl StreamConsumerActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: ConsumerPhase,
        reason: Reason,
        outcome: Option<Outcome>,
    ) {
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            let phase = |phase| match phase {
                ConsumerPhase::Opening => Phase::Opening,
                ConsumerPhase::Open => Phase::Open,
                ConsumerPhase::Closing => Phase::Closing,
                ConsumerPhase::Finished => Phase::Finished,
            };
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::StreamConsumerActor,
                protocol: Protocol::StreamConsumer,
                operation: self
                    .operation
                    .map(|operation| operation.0[..16].try_into().expect("operation identity")),
                generation: self
                    .incarnation
                    .map(|incarnation| incarnation.authority_epoch),
                incarnation: self.incarnation.map(|incarnation| incarnation.revision),
                previous: phase(previous),
                next: phase(next),
                reason,
                outcome,
            });
        }
    }

    fn opened(&mut self, ctx: &Ctx<'_>, result: Result<StreamOpenGrant, DataPlaneError>) {
        if self.phase != ConsumerPhase::Opening {
            return;
        }
        match result {
            Ok(grant) => match attach_mapped(&self.arena, grant.ring, Role::Consumer) {
                Ok(endpoint) => {
                    self.operation = Some(grant.operation);
                    self.incarnation = Some(grant.incarnation);
                    self.host_binding = Some(grant.host_binding);
                    self.endpoint = Some(endpoint);
                    grant.cancellation.disarm();
                    self.enter_phase(ctx, ConsumerPhase::Open, Reason::GrantReceived, None);
                    self.drain(ctx);
                }
                Err(error) => self.finish(
                    ctx,
                    Err(DataPlaneError::StreamFault(format!(
                        "attach stream collector: {error:?}"
                    ))),
                    false,
                ),
            },
            Err(error) => self.finish(ctx, Err(error), false),
        }
    }

    fn wake(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.phase != ConsumerPhase::Open {
            return;
        }
        match result {
            Ok(()) => self.drain(ctx),
            Err(error) => self.finish(ctx, Err(error), false),
        }
    }

    fn close_completed(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.phase != ConsumerPhase::Closing {
            return;
        }
        self.close_operation = None;
        let pending = self
            .pending_result
            .take()
            .expect("closing retains terminal result");
        self.complete_now(ctx, result.and(pending));
    }
    fn send_control(&mut self, ctx: &Ctx<'_>, binding: ActorAddress, message: HostStreamIn) {
        if let Err(error) = ctx.send(
            self.child_session,
            ChildSessionIn::StreamControl { binding, message },
        ) {
            self.complete_now(ctx, Err(DataPlaneError::SessionFailed(error.to_string())));
        }
    }

    fn complete_now(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.phase == ConsumerPhase::Finished {
            return;
        }
        self.enter_phase(
            ctx,
            ConsumerPhase::Finished,
            Reason::Completed,
            Some(operation_outcome(&result)),
        );
        if let Some(operation) = self.operation {
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::OperationDone { operation },
            );
        }
        let _ = self.completion.complete(result);
        ctx.stop_self();
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>, clean: bool) {
        if matches!(self.phase, ConsumerPhase::Finished | ConsumerPhase::Closing) {
            return;
        }
        if clean && let Some(host_binding) = self.host_binding {
            self.pending_result = Some(result);
            self.enter_phase(ctx, ConsumerPhase::Closing, Reason::CloseRequested, None);
            match ctx.spawn(StreamConsumerCloseReply {
                child_session: self.child_session,
                binding: host_binding,
                reply_to: Some(ctx.self_addr()),
            }) {
                Ok(operation) => self.close_operation = Some(operation),
                Err(error) => {
                    self.complete_now(ctx, Err(DataPlaneError::SessionFailed(error.to_string())))
                }
            }
            return;
        }
        if let Some(host_binding) = self.host_binding {
            self.send_control(
                ctx,
                host_binding,
                HostStreamIn::Close {
                    clean: false,
                    reply_to: None,
                },
            );
        }
        self.complete_now(ctx, result);
    }

    fn drain(&mut self, ctx: &Ctx<'_>) {
        while self.phase == ConsumerPhase::Open {
            let step = consume_next_record(
                self.endpoint
                    .as_mut()
                    .expect("collector endpoint is installed before drain"),
                self.consumer.as_ref(),
            );
            match step {
                Ok(ConsumerDrainStep::Empty) => {
                    if let Some(host_binding) = self.host_binding {
                        self.send_control(
                            ctx,
                            host_binding,
                            HostStreamIn::WaitData {
                                reply_to: ctx.self_addr(),
                            },
                        );
                    }
                    return;
                }
                Ok(ConsumerDrainStep::Data) => {
                    if let Some(host_binding) = self.host_binding {
                        self.send_control(ctx, host_binding, HostStreamIn::CapacityAvailable);
                    }
                }
                Ok(ConsumerDrainStep::Eof) => {
                    if let Some(host_binding) = self.host_binding {
                        self.send_control(ctx, host_binding, HostStreamIn::CapacityAvailable);
                    }
                    self.finish(ctx, Ok(()), true);
                    return;
                }
                Ok(ConsumerDrainStep::Fault(reason)) => {
                    self.finish(ctx, Err(DataPlaneError::StreamFault(reason)), false);
                    return;
                }
                Err(error) => {
                    self.finish(ctx, Err(error), false);
                    return;
                }
            }
        }
    }
}

impl ActorInterface for StreamConsumerActor {
    type Incoming = ChildStreamIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        ctx.watch(self.child_session);
        self.enter_phase(ctx, ConsumerPhase::Opening, Reason::Started, None);
        if let Err(error) = ctx.send(
            self.child_session,
            ChildSessionIn::OpenReadStream {
                path: self.path.clone(),
                reply_to: ctx.self_addr(),
                replace: false,
            },
        ) {
            self.complete_now(ctx, Err(DataPlaneError::SessionFailed(error.to_string())));
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ChildStreamIn) {
        match message {
            ChildStreamIn::Opened(result) => self.opened(ctx, result),
            ChildStreamIn::Wake(result) => self.wake(ctx, result),
            ChildStreamIn::CloseCompleted(result) => self.close_completed(ctx, result),
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, _exited: ActorExited) {
        self.complete_now(ctx, Err(DataPlaneError::PeerLost));
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if let Some(operation) = self.close_operation.take() {
            let _ = ctx.stop_actor(operation);
        }
        if self.phase != ConsumerPhase::Finished {
            if let Some(host_binding) = self.host_binding {
                self.send_control(
                    ctx,
                    host_binding,
                    HostStreamIn::Close {
                        clean: false,
                        reply_to: None,
                    },
                );
            } else {
                let _ = ctx.send(
                    self.child_session,
                    ChildSessionIn::CancelStream {
                        reply_to: ctx.self_addr(),
                    },
                );
            }
            self.complete_now(ctx, Err(DataPlaneError::OperationCancelled));
        }
    }
}

// A fresh reply identity prevents an old WaitData wake from acknowledging Close.
// This actor only translates one response; the consumer owns the close phase.
struct StreamConsumerCloseReply {
    child_session: ActorAddress,
    binding: ActorAddress,
    reply_to: Option<ActorAddress>,
}

impl StreamConsumerCloseReply {
    fn complete(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if let Some(reply_to) = self.reply_to.take() {
            let _ = ctx.send(reply_to, ChildStreamIn::CloseCompleted(result));
        }
        ctx.stop_self();
    }
}

impl ActorInterface for StreamConsumerCloseReply {
    type Incoming = ChildStreamIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        ctx.watch(self.child_session);
        ctx.watch(self.reply_to.expect("close reply owner"));
        if let Err(error) = ctx.send(
            self.child_session,
            ChildSessionIn::StreamControl {
                binding: self.binding,
                message: HostStreamIn::Close {
                    clean: true,
                    reply_to: Some(ctx.self_addr()),
                },
            },
        ) {
            self.complete(ctx, Err(DataPlaneError::SessionFailed(error.to_string())));
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ChildStreamIn) {
        if let ChildStreamIn::Wake(result) = message {
            self.complete(ctx, result);
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, _exited: ActorExited) {
        self.complete(ctx, Err(DataPlaneError::OperationCancelled));
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        self.complete(ctx, Err(DataPlaneError::OperationCancelled));
    }
}

pub struct ChildDataPlaneSessionActor {
    runtime: Runtime,
    host_session: ActorAddress,
    arena_generation: u64,
    session_capability: SessionCapability,
    child_node: Option<[u8; 32]>,
    session_generation: Option<u64>,
    attach_reply: Option<ActorAddress>,
    operations: HashMap<ActorAddress, OwnedOperation>,
    state: ChildSessionState,
    namespace_operations: HashSet<ActorAddress>,
    stream_waiters: HashMap<ActorAddress, ActorAddress>,
    pending_blob_releases: HashSet<(ActorAddress, crate::ids::BlobLeaseId, u64)>,
    deferred_blob_opens: VecDeque<DeferredOpen>,
    close_replies: Vec<ActorAddress>,
    close_result: Option<Result<(), DataPlaneError>>,
    host_close_result: Option<Result<(), DataPlaneError>>,
    transitions: Option<TransitionEmitter>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OperationKind {
    Descriptor,
    Stream,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OperationDelivery {
    AwaitingGrant,
    Forwarded,
}

struct OwnedOperation {
    caller: ActorAddress,
    kind: OperationKind,
    delivery: OperationDelivery,
}

impl ChildDataPlaneSessionActor {
    pub fn state(&self) -> ChildSessionState {
        self.state
    }

    fn enter_state(
        &mut self,
        ctx: &Ctx<'_>,
        next: ChildSessionState,
        reason: Reason,
        outcome: Option<Outcome>,
    ) {
        let previous = self.state;
        self.state = next;
        if let Some(transitions) = &self.transitions {
            let phase = |state| match state {
                ChildSessionState::Attaching => Phase::Attaching,
                ChildSessionState::Running => Phase::Running,
                ChildSessionState::Closing => Phase::Closing,
                ChildSessionState::Closed => Phase::Closed,
            };
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::ChildDataPlaneSessionActor,
                protocol: Protocol::ChildSession,
                operation: None,
                generation: self.session_generation,
                incarnation: None,
                previous: phase(previous),
                next: phase(next),
                reason,
                outcome,
            });
        }
    }

    fn advance_close(&mut self, ctx: &Ctx<'_>) {
        if self.state != ChildSessionState::Closing || !self.operations.is_empty() {
            return;
        }
        let Some(result) = self.host_close_result.take() else {
            return;
        };
        // The host close acknowledgement transfers all remaining lease ownership
        // to host teardown, including release acknowledgements overtaken by close.
        self.pending_blob_releases.clear();
        self.enter_state(
            ctx,
            ChildSessionState::Closed,
            Reason::HostClosed,
            Some(operation_outcome(&result)),
        );
        self.close_result = Some(result.clone());
        for reply_to in self.close_replies.drain(..) {
            let _ = ctx.send(reply_to, result.clone());
        }
    }

    fn reject_pending(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        if let Some(reply_to) = self.attach_reply.take() {
            let _ = ctx.send(reply_to, Err::<u64, _>(error.clone()));
        }
        for &operation in self.operations.keys() {
            let _ = ctx.send(operation, ChildOperationIn::Failed(error.clone()));
        }
        for reply_to in self.namespace_operations.drain() {
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::CancelNamespace {
                    operation: reply_to,
                },
            );
            let _ = ctx.send(reply_to, Err::<NamespaceOperationResult, _>(error.clone()));
        }
        for deferred in self.deferred_blob_opens.drain(..) {
            let _ = ctx.send(
                deferred.reply_to,
                Err::<DescriptorOpenGrant, _>(error.clone()),
            );
        }
        for (reply_to, _) in self.stream_waiters.drain() {
            let _ = ctx.send(reply_to, ChildStreamIn::Wake(Err(error.clone())));
        }
    }

    fn begin_close(&mut self, ctx: &Ctx<'_>, reply_to: Option<ActorAddress>) {
        if self.state == ChildSessionState::Closed {
            if let Some(reply_to) = reply_to {
                let _ = ctx.send(reply_to, self.close_result.clone().unwrap_or(Ok(())));
            }
            return;
        }
        if let Some(reply_to) = reply_to
            && !self.close_replies.contains(&reply_to)
        {
            self.close_replies.push(reply_to);
        }
        if self.state == ChildSessionState::Closing {
            return;
        }
        self.enter_state(
            ctx,
            ChildSessionState::Closing,
            Reason::CloseRequested,
            None,
        );
        self.reject_pending(ctx, DataPlaneError::SessionNotRunning);
        if let Err(error) = ctx.send(
            self.host_session,
            HostSessionIn::CloseChild {
                child_session: ctx.self_addr(),
            },
        ) {
            self.host_closed(ctx, Err(DataPlaneError::SessionFailed(error.to_string())));
        }
    }

    fn attached(&mut self, ctx: &Ctx<'_>, generation: u64) {
        if self.state != ChildSessionState::Attaching {
            return;
        }
        self.session_generation = Some(generation);
        self.enter_state(ctx, ChildSessionState::Running, Reason::Attached, None);
        if let Some(reply_to) = self.attach_reply.take() {
            let _ = ctx.send(reply_to, Ok::<_, DataPlaneError>(generation));
        }
    }

    fn attachment_failed(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        if self.state != ChildSessionState::Attaching {
            return;
        }
        self.reject_pending(ctx, error.clone());
        self.close_result = Some(Err(error));
        self.enter_state(
            ctx,
            ChildSessionState::Closed,
            Reason::AttachmentFailed,
            Some(Outcome::Failed),
        );
    }

    fn own_operation(
        &mut self,
        ctx: &Ctx<'_>,
        operation: ActorAddress,
        caller: ActorAddress,
        kind: OperationKind,
    ) {
        self.operations.insert(
            operation,
            OwnedOperation {
                caller,
                kind,
                delivery: OperationDelivery::AwaitingGrant,
            },
        );
        ctx.watch(operation);
    }

    fn open_descriptor(&mut self, ctx: &Ctx<'_>, request: DeferredOpen) {
        if self.state != ChildSessionState::Running {
            let _ = ctx.send(
                request.reply_to,
                Err::<DescriptorOpenGrant, _>(DataPlaneError::SessionNotRunning),
            );
            return;
        }
        if !self.pending_blob_releases.is_empty() {
            self.deferred_blob_opens.push_back(request);
            return;
        }
        let reply_to = request.reply_to;
        let actor = DescriptorOpenOperationActor {
            runtime: self.runtime.clone(),
            host_session: self.host_session,
            child_session: ctx.self_addr(),
            path: request.path,
            options: request.options,
            policy: request.policy,
            open_reply: Some(reply_to),
            finish_reply: None,
            grant: None,
            state: WriteOperationState::Opening,
            transitions: self.transitions.clone(),
            incarnation: None,
        };
        match ctx.spawn(actor) {
            Ok(operation) => {
                self.own_operation(ctx, operation, reply_to, OperationKind::Descriptor)
            }
            Err(error) => {
                let _ = ctx.send(
                    reply_to,
                    Err::<DescriptorOpenGrant, _>(DataPlaneError::SessionFailed(error.to_string())),
                );
            }
        }
    }

    fn start_stream_open(
        &mut self,
        ctx: &Ctx<'_>,
        path: DataPath,
        reply_to: ActorAddress,
        role: Role,
        replace: bool,
    ) {
        if self.state != ChildSessionState::Running {
            let _ = ctx.send(
                reply_to,
                ChildStreamIn::Opened(Err(DataPlaneError::SessionNotRunning)),
            );
            return;
        }
        let actor = StreamOpenOperationActor {
            runtime: self.runtime.clone(),
            host_session: self.host_session,
            child_session: ctx.self_addr(),
            path,
            role,
            replace,
            reply_to,
            phase: StreamOpenPhase::Opening,
            transitions: self.transitions.clone(),
            incarnation: None,
        };
        match ctx.spawn(actor) {
            Ok(operation) => self.own_operation(ctx, operation, reply_to, OperationKind::Stream),
            Err(error) => {
                let _ = ctx.send(
                    reply_to,
                    ChildStreamIn::Opened(Err(DataPlaneError::SessionFailed(error.to_string()))),
                );
            }
        }
    }

    fn namespace_requested(
        &mut self,
        ctx: &Ctx<'_>,
        request: NamespaceOperation,
        reply_to: ActorAddress,
    ) {
        if self.state != ChildSessionState::Running {
            let _ = ctx.send(
                reply_to,
                Err::<NamespaceOperationResult, _>(DataPlaneError::SessionNotRunning),
            );
            return;
        }
        if !self.namespace_operations.insert(reply_to) {
            return;
        }
        ctx.watch(reply_to);
        if let Err(error) = ctx.send(
            self.host_session,
            HostSessionIn::Namespace {
                operation: reply_to,
                request,
                child_session: ctx.self_addr(),
            },
        ) {
            self.namespace_resolved(
                ctx,
                reply_to,
                Err(DataPlaneError::SessionFailed(error.to_string())),
            );
        }
    }

    fn namespace_resolved(
        &mut self,
        ctx: &Ctx<'_>,
        reply_to: ActorAddress,
        result: Result<NamespaceOperationResult, DataPlaneError>,
    ) {
        if self.namespace_operations.remove(&reply_to) {
            let _ = ctx.send(reply_to, result);
        }
    }

    fn cancel_namespace(&mut self, ctx: &Ctx<'_>, reply_to: ActorAddress) {
        if self.namespace_operations.remove(&reply_to) {
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::CancelNamespace {
                    operation: reply_to,
                },
            );
            let _ = ctx.send(
                reply_to,
                Err::<NamespaceOperationResult, _>(DataPlaneError::OperationCancelled),
            );
        }
    }

    fn cancel_open(&mut self, ctx: &Ctx<'_>, reply_to: ActorAddress, kind: OperationKind) {
        self.deferred_blob_opens.retain(|request| {
            if request.reply_to != reply_to {
                return true;
            }
            let _ = ctx.send(
                reply_to,
                Err::<DescriptorOpenGrant, _>(DataPlaneError::OperationCancelled),
            );
            false
        });
        for (&operation, owned) in &self.operations {
            if owned.caller == reply_to && owned.kind == kind {
                let _ = ctx.send(
                    operation,
                    ChildOperationIn::Failed(DataPlaneError::OperationCancelled),
                );
            }
        }
    }

    fn stream_wake(
        &mut self,
        ctx: &Ctx<'_>,
        reply_to: ActorAddress,
        result: Result<(), DataPlaneError>,
    ) {
        if self.stream_waiters.remove(&reply_to).is_some() {
            let _ = ctx.send(reply_to, ChildStreamIn::Wake(result));
        }
    }

    fn stream_control(&mut self, ctx: &Ctx<'_>, binding: ActorAddress, message: HostStreamIn) {
        let reply_to = message.child_reply_to();
        if self.state != ChildSessionState::Running {
            if let Some(reply_to) = reply_to {
                let _ = ctx.send(
                    reply_to,
                    ChildStreamIn::Wake(Err(DataPlaneError::SessionNotRunning)),
                );
            }
            return;
        }
        if let Some(reply_to) = reply_to {
            self.stream_waiters.insert(reply_to, binding);
        }
        if let Err(error) = ctx.send(
            self.host_session,
            HostSessionIn::StreamControl { binding, message },
        ) && let Some(reply_to) = reply_to
        {
            self.stream_wake(
                ctx,
                reply_to,
                Err(DataPlaneError::SessionFailed(error.to_string())),
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
        if self.state != ChildSessionState::Running
            || !self
                .pending_blob_releases
                .insert((binding, lease_id, generation))
        {
            return;
        }
        if let Err(error) = ctx.send(
            self.host_session,
            HostSessionIn::ReleaseBlob {
                binding,
                lease_id,
                generation,
            },
        ) {
            self.begin_close(ctx, None);
            self.host_closed(ctx, Err(DataPlaneError::SessionFailed(error.to_string())));
        }
    }

    fn blob_released(
        &mut self,
        ctx: &Ctx<'_>,
        binding: ActorAddress,
        lease_id: crate::ids::BlobLeaseId,
        generation: u64,
    ) {
        if !self
            .pending_blob_releases
            .remove(&(binding, lease_id, generation))
        {
            return;
        }
        if self.pending_blob_releases.is_empty() {
            while let Some(request) = self.deferred_blob_opens.pop_front() {
                self.open_descriptor(ctx, request);
            }
        }
    }

    fn forward_operation(
        &mut self,
        ctx: &Ctx<'_>,
        operation: ActorAddress,
        message: ChildOperationIn,
    ) {
        let Some(owned) = self.operations.get_mut(&operation) else {
            // The terminal handler already cancelled or transferred its grant.
            // Re-cancelling a duplicate would revoke a successfully opened descriptor.
            return;
        };
        let grant = matches!(
            &message,
            ChildOperationIn::ReadOpened { .. }
                | ChildOperationIn::WriteOpened { .. }
                | ChildOperationIn::StreamOpened { .. }
        );
        if grant && owned.delivery == OperationDelivery::Forwarded {
            return;
        }
        let incarnation = match &message {
            ChildOperationIn::StreamOpened { incarnation, .. } => Some(incarnation.revision),
            _ => None,
        };
        if ctx.send(operation, message).is_err() {
            let _ = ctx.send(self.host_session, HostSessionIn::CancelOpen { operation });
            self.operation_done(ctx, operation);
            return;
        }
        if grant {
            owned.delivery = OperationDelivery::Forwarded;
            if let Some(transitions) = &self.transitions {
                transitions.emit(TransitionRecord {
                    actor: ctx.self_addr(),
                    actor_type: ActorType::ChildDataPlaneSessionActor,
                    protocol: Protocol::ChildSession,
                    operation: Some(operation.0[..16].try_into().expect("operation identity")),
                    generation: self.session_generation,
                    incarnation,
                    previous: Phase::Opening,
                    next: Phase::Forwarding,
                    reason: Reason::Forwarded,
                    outcome: None,
                });
            }
        }
    }

    fn operation_done(&mut self, ctx: &Ctx<'_>, operation: ActorAddress) {
        self.operations.remove(&operation);
        self.advance_close(ctx);
    }

    fn host_closed(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.state != ChildSessionState::Closing {
            return;
        }
        if self.host_close_result.is_none() {
            self.host_close_result = Some(result);
        }
        self.advance_close(ctx);
    }

    fn host_unavailable(&mut self, ctx: &Ctx<'_>, host_session: ActorAddress) {
        if host_session != self.host_session || self.state == ChildSessionState::Closed {
            return;
        }
        self.enter_state(
            ctx,
            ChildSessionState::Closing,
            Reason::PeerTerminated,
            None,
        );
        self.reject_pending(ctx, DataPlaneError::PeerLost);
        self.host_close_result = Some(Err(DataPlaneError::PeerLost));
        self.advance_close(ctx);
    }

    fn actor_exited(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if exited.addr == self.host_session {
            self.host_unavailable(ctx, exited.addr);
        } else if self.operations.contains_key(&exited.addr) {
            self.operation_done(ctx, exited.addr);
        } else {
            self.cancel_namespace(ctx, exited.addr);
            self.cancel_open(ctx, exited.addr, OperationKind::Descriptor);
            self.cancel_open(ctx, exited.addr, OperationKind::Stream);
            self.stream_waiters.remove(&exited.addr);
        }
    }
}

impl ActorInterface for ChildDataPlaneSessionActor {
    type Incoming = ChildSessionIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        ctx.watch(self.host_session);
        self.enter_state(ctx, ChildSessionState::Attaching, Reason::Started, None);
        if let Err(error) = ctx.send(
            self.host_session,
            HostSessionIn::Attach {
                child_session: ctx.self_addr(),
                arena_generation: self.arena_generation,
                session_capability: self.session_capability,
                child_node: self.child_node,
            },
        ) {
            self.attachment_failed(ctx, DataPlaneError::SessionFailed(error.to_string()));
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ChildSessionIn) {
        match message {
            ChildSessionIn::Attached { session_generation } => {
                self.attached(ctx, session_generation)
            }
            ChildSessionIn::AttachmentFailed { error } => self.attachment_failed(ctx, error),
            ChildSessionIn::HostUnavailable { host_session } => {
                self.host_unavailable(ctx, host_session)
            }
            ChildSessionIn::Open {
                path,
                options,
                policy,
                reply_to,
            } => self.open_descriptor(
                ctx,
                DeferredOpen {
                    path,
                    options,
                    policy,
                    reply_to,
                },
            ),
            ChildSessionIn::Namespace { request, reply_to } => {
                self.namespace_requested(ctx, request, reply_to)
            }
            ChildSessionIn::CancelNamespace { reply_to } => self.cancel_namespace(ctx, reply_to),
            ChildSessionIn::NamespaceResolved { reply_to, result } => {
                self.namespace_resolved(ctx, reply_to, result)
            }
            ChildSessionIn::CancelOpen { reply_to } => {
                self.cancel_open(ctx, reply_to, OperationKind::Descriptor)
            }
            ChildSessionIn::OpenReadStream {
                path,
                reply_to,
                replace,
            } => self.start_stream_open(ctx, path, reply_to, Role::Consumer, replace),
            ChildSessionIn::CancelStream { reply_to } => {
                self.cancel_open(ctx, reply_to, OperationKind::Stream)
            }
            ChildSessionIn::StreamWake { reply_to, result } => {
                self.stream_wake(ctx, reply_to, result)
            }
            ChildSessionIn::StreamControl { binding, message } => {
                self.stream_control(ctx, binding, message)
            }
            ChildSessionIn::BlobReleased {
                binding,
                lease_id,
                generation,
            } => self.blob_released(ctx, binding, lease_id, generation),
            ChildSessionIn::ReleaseBlob {
                binding,
                lease_id,
                generation,
            } => self.release_blob(ctx, binding, lease_id, generation),
            ChildSessionIn::BlobOpened {
                operation,
                host_binding,
                lease,
                metadata,
            } => self.forward_operation(
                ctx,
                operation,
                ChildOperationIn::ReadOpened {
                    host_binding,
                    lease,
                    metadata,
                },
            ),
            ChildSessionIn::WriteBlobOpened {
                operation,
                host_binding,
                lease,
                metadata,
            } => self.forward_operation(
                ctx,
                operation,
                ChildOperationIn::WriteOpened {
                    host_binding,
                    lease,
                    metadata,
                },
            ),
            ChildSessionIn::StreamOpened {
                operation,
                host_binding,
                incarnation,
                ring,
                role,
            } => self.forward_operation(
                ctx,
                operation,
                ChildOperationIn::StreamOpened {
                    host_binding,
                    incarnation,
                    ring,
                    role,
                },
            ),
            ChildSessionIn::OperationFailed { operation, error } => {
                self.forward_operation(ctx, operation, ChildOperationIn::Failed(error))
            }
            ChildSessionIn::WritePublished { operation } => {
                self.forward_operation(ctx, operation, ChildOperationIn::WritePublished)
            }
            ChildSessionIn::WriteAborted { operation } => {
                self.forward_operation(ctx, operation, ChildOperationIn::WriteAborted)
            }
            ChildSessionIn::OperationDone { operation } => self.operation_done(ctx, operation),
            ChildSessionIn::Close { reply_to } => self.begin_close(ctx, reply_to),
            ChildSessionIn::CloseCompleted { result } => self.host_closed(ctx, result),
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        self.actor_exited(ctx, exited);
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        self.reject_pending(ctx, DataPlaneError::OperationCancelled);
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::CloseChild {
                child_session: ctx.self_addr(),
            },
        );
        for reply_to in self.close_replies.drain(..) {
            let _ = ctx.send(reply_to, Err::<(), _>(DataPlaneError::OperationCancelled));
        }
    }
}

struct DeferredOpen {
    path: DataPath,
    options: OpenOptions,
    policy: OpenPolicy,
    reply_to: ActorAddress,
}

#[derive(Clone)]
enum ChildOperationIn {
    ReadOpened {
        host_binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    WriteOpened {
        host_binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    StreamOpened {
        host_binding: ActorAddress,
        incarnation: StreamIncarnation,
        ring: RingHandle,
        role: Role,
    },
    Failed(DataPlaneError),
    SealRequested {
        reply_to: Option<ActorAddress>,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    AbortRequested {
        reply_to: Option<ActorAddress>,
        lease: BlobLease,
    },
    WritePublished,
    WriteAborted,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamOpenPhase {
    Opening,
    Forwarding,
    Finished,
}

struct StreamOpenOperationActor {
    runtime: Runtime,
    host_session: ActorAddress,
    child_session: ActorAddress,
    path: DataPath,
    role: Role,
    reply_to: ActorAddress,
    replace: bool,
    phase: StreamOpenPhase,
    incarnation: Option<StreamIncarnation>,
    transitions: Option<TransitionEmitter>,
}

impl StreamOpenOperationActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: StreamOpenPhase,
        reason: Reason,
        outcome: Option<Outcome>,
    ) {
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            let phase = |phase| match phase {
                StreamOpenPhase::Opening => Phase::Opening,
                StreamOpenPhase::Forwarding => Phase::Forwarding,
                StreamOpenPhase::Finished => Phase::Finished,
            };
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::StreamOpenOperationActor,
                protocol: Protocol::StreamOpenOperation,
                operation: Some(
                    ctx.self_addr().0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: self.incarnation.map(|value| value.authority_epoch),
                incarnation: self.incarnation.map(|value| value.revision),
                previous: phase(previous),
                next: phase(next),
                reason,
                outcome,
            });
        }
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<StreamOpenGrant, DataPlaneError>) {
        if self.phase == StreamOpenPhase::Finished {
            return;
        }
        let acknowledgement = result
            .as_ref()
            .ok()
            .map(|grant| (grant.host_binding, grant.incarnation));
        let outcome = operation_outcome(&result);
        self.enter_phase(ctx, StreamOpenPhase::Forwarding, Reason::Forwarded, None);
        if ctx
            .send(self.reply_to, ChildStreamIn::Opened(result))
            .is_ok()
        {
            if let Some((host_binding, incarnation)) = acknowledgement {
                let _ = ctx.send(
                    self.host_session,
                    HostSessionIn::StreamOpenForwarded {
                        operation: ctx.self_addr(),
                        host_binding,
                        incarnation,
                    },
                );
            } else {
                let _ = ctx.send(
                    self.host_session,
                    HostSessionIn::CancelOpen {
                        operation: ctx.self_addr(),
                    },
                );
            }
            self.enter_phase(
                ctx,
                StreamOpenPhase::Finished,
                Reason::Completed,
                Some(outcome),
            );
        } else {
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::CancelOpen {
                    operation: ctx.self_addr(),
                },
            );
            self.enter_phase(
                ctx,
                StreamOpenPhase::Finished,
                Reason::Cancelled,
                Some(Outcome::Cancelled),
            );
        }
        let _ = ctx.send(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: ctx.self_addr(),
            },
        );
        ctx.stop_self();
    }

    fn stream_opened(
        &mut self,
        ctx: &Ctx<'_>,
        host_binding: ActorAddress,
        incarnation: StreamIncarnation,
        ring: RingHandle,
        role: Role,
    ) {
        if self.phase != StreamOpenPhase::Opening {
            return;
        }
        if role != self.role {
            self.finish(
                ctx,
                Err(DataPlaneError::StreamFault(
                    "host opened stream with the wrong ring role".to_owned(),
                )),
            );
            return;
        }
        self.incarnation = Some(incarnation);
        let cancellation = Arc::new(DescriptorGrantCancellation {
            runtime: self.runtime.clone(),
            action: DescriptorGrantCancellationAction::HostOpen {
                host_session: self.host_session,
                operation: ctx.self_addr(),
            },
            armed: AtomicBool::new(true),
        });
        self.finish(
            ctx,
            Ok(StreamOpenGrant {
                operation: ctx.self_addr(),
                host_binding,
                incarnation,
                ring,
                cancellation,
            }),
        );
    }
}

impl ActorInterface for StreamOpenOperationActor {
    type Incoming = ChildOperationIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        ctx.watch(self.child_session);
        ctx.watch(self.reply_to);
        self.enter_phase(ctx, StreamOpenPhase::Opening, Reason::Started, None);
        let access = match self.role {
            Role::Consumer => AccessMode::ReadOnly,
            Role::Producer => AccessMode::WriteOnly,
        };
        let message = HostSessionIn::Open {
            path: self.path.clone(),
            options: OpenOptions {
                access,
                ..OpenOptions::default()
            },
            policy: OpenPolicy::EnsureStream {
                replace: self.replace,
            },
            child_session: self.child_session,
            operation: ctx.self_addr(),
        };
        if let Err(error) = ctx.send(self.host_session, message) {
            self.finish(ctx, Err(DataPlaneError::SessionFailed(error.to_string())));
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ChildOperationIn) {
        match message {
            ChildOperationIn::StreamOpened {
                host_binding,
                incarnation,
                ring,
                role,
            } => self.stream_opened(ctx, host_binding, incarnation, ring, role),
            ChildOperationIn::Failed(error) => self.finish(ctx, Err(error)),
            _ => {}
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, _exited: ActorExited) {
        self.finish(ctx, Err(DataPlaneError::OperationCancelled));
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.phase != StreamOpenPhase::Finished {
            self.finish(ctx, Err(DataPlaneError::OperationCancelled));
        }
    }
}

struct RuntimeLeaseReleaser {
    runtime: Runtime,
    child_session: ActorAddress,
    host_binding: ActorAddress,
}

impl LeaseReleaser for RuntimeLeaseReleaser {
    fn release(&self, lease: BlobLease) {
        let _ = self.runtime.send_to(
            self.child_session,
            ChildSessionIn::ReleaseBlob {
                binding: self.host_binding,
                lease_id: lease.lease_id,
                generation: lease.generation,
            },
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum WriteOperationState {
    Opening,
    Filling,
    Sealing,
    Aborting,
    Finished,
}

struct DescriptorOpenOperationActor {
    runtime: Runtime,
    host_session: ActorAddress,
    child_session: ActorAddress,
    path: DataPath,
    options: OpenOptions,
    policy: OpenPolicy,
    open_reply: Option<ActorAddress>,
    finish_reply: Option<ActorAddress>,
    grant: Option<(ActorAddress, BlobLease, BlobMetadata)>,
    state: WriteOperationState,
    incarnation: Option<StreamIncarnation>,
    transitions: Option<TransitionEmitter>,
}

impl DescriptorOpenOperationActor {
    fn enter_state(
        &mut self,
        ctx: &Ctx<'_>,
        next: WriteOperationState,
        reason: Reason,
        outcome: Option<Outcome>,
    ) {
        let previous = self.state;
        self.state = next;
        if let Some(transitions) = &self.transitions {
            let phase = |state| match state {
                WriteOperationState::Opening => Phase::Opening,
                WriteOperationState::Filling => Phase::Filling,
                WriteOperationState::Sealing => Phase::Sealing,
                WriteOperationState::Aborting => Phase::Aborting,
                WriteOperationState::Finished => Phase::Finished,
            };
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::DescriptorOpenOperationActor,
                protocol: Protocol::DescriptorOperation,
                operation: Some(
                    ctx.self_addr().0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: self
                    .grant
                    .as_ref()
                    .map(|(_, lease, _)| lease.generation)
                    .or_else(|| {
                        self.incarnation
                            .map(|incarnation| incarnation.authority_epoch)
                    }),
                incarnation: self.incarnation.map(|value| value.revision),
                previous: phase(previous),
                next: phase(next),
                reason,
                outcome,
            });
        }
    }

    fn operation_done(&self, ctx: &Ctx<'_>) {
        let _ = ctx.send(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: ctx.self_addr(),
            },
        );
    }

    fn finish_open(
        &mut self,
        ctx: &Ctx<'_>,
        result: Result<DescriptorOpenGrant, DataPlaneError>,
        keep_alive: bool,
    ) {
        let outcome = operation_outcome(&result);
        let acknowledgement = match &result {
            Ok(DescriptorOpenGrant::Stream {
                host_binding,
                incarnation,
                ..
            }) => Some((*host_binding, *incarnation)),
            _ => None,
        };
        let Some(reply_to) = self.open_reply.take() else {
            return;
        };
        let delivered = ctx.send(reply_to, result).is_ok();
        if delivered {
            if let Some((host_binding, incarnation)) = acknowledgement {
                let _ = ctx.send(
                    self.host_session,
                    HostSessionIn::StreamOpenForwarded {
                        operation: ctx.self_addr(),
                        host_binding,
                        incarnation,
                    },
                );
            }
        } else {
            self.fail(ctx, DataPlaneError::OperationCancelled);
            return;
        }
        if !keep_alive {
            self.enter_state(
                ctx,
                WriteOperationState::Finished,
                Reason::Completed,
                Some(outcome),
            );
            self.operation_done(ctx);
            ctx.stop_self();
        }
    }

    fn finish_write(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.state == WriteOperationState::Finished {
            return;
        }
        let outcome = operation_outcome(&result);
        self.enter_state(
            ctx,
            WriteOperationState::Finished,
            Reason::Completed,
            Some(outcome),
        );
        if let Some(reply_to) = self.finish_reply.take() {
            let _ = ctx.send(reply_to, result);
        }
        self.operation_done(ctx);
        ctx.stop_self();
    }

    fn fail(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        if self.state == WriteOperationState::Finished {
            return;
        }
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::CancelOpen {
                operation: ctx.self_addr(),
            },
        );
        if let Some(reply_to) = self.open_reply.take() {
            let _ = ctx.send(reply_to, Err::<DescriptorOpenGrant, _>(error.clone()));
        }
        self.finish_write(ctx, Err(error));
    }

    fn grant_cancellation(
        &self,
        action: DescriptorGrantCancellationAction,
    ) -> Arc<DescriptorGrantCancellation> {
        Arc::new(DescriptorGrantCancellation {
            runtime: self.runtime.clone(),
            action,
            armed: AtomicBool::new(true),
        })
    }

    fn read_opened(
        &mut self,
        ctx: &Ctx<'_>,
        host_binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    ) {
        if self.state != WriteOperationState::Opening {
            return;
        }
        let cancellation = self.grant_cancellation(DescriptorGrantCancellationAction::HostOpen {
            host_session: self.host_session,
            operation: ctx.self_addr(),
        });
        self.finish_open(
            ctx,
            Ok(DescriptorOpenGrant::ReadBlob {
                host_binding,
                lease,
                metadata,
                cancellation,
            }),
            false,
        );
    }

    fn write_opened(
        &mut self,
        ctx: &Ctx<'_>,
        host_binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    ) {
        if self.state != WriteOperationState::Opening {
            return;
        }
        self.grant = Some((host_binding, lease, metadata.clone()));
        self.enter_state(
            ctx,
            WriteOperationState::Filling,
            Reason::GrantReceived,
            None,
        );
        let cancellation = self.grant_cancellation(DescriptorGrantCancellationAction::WriteBlob {
            operation: ctx.self_addr(),
            lease,
        });
        self.finish_open(
            ctx,
            Ok(DescriptorOpenGrant::WriteBlob {
                operation: ctx.self_addr(),
                lease,
                metadata,
                access: self.options.access,
                cancellation,
            }),
            true,
        );
    }

    fn stream_opened(
        &mut self,
        ctx: &Ctx<'_>,
        host_binding: ActorAddress,
        incarnation: StreamIncarnation,
        ring: RingHandle,
        role: Role,
    ) {
        if self.state != WriteOperationState::Opening {
            return;
        }
        let expected_role = match self.options.access {
            AccessMode::ReadOnly => Role::Consumer,
            AccessMode::WriteOnly => Role::Producer,
            AccessMode::ReadWrite => {
                self.fail(
                    ctx,
                    DataPlaneError::Unsupported(
                        "read-write stream descriptors are not supported".to_owned(),
                    ),
                );
                return;
            }
        };
        if role != expected_role {
            self.fail(
                ctx,
                DataPlaneError::StreamFault(
                    "host opened stream with the wrong ring role".to_owned(),
                ),
            );
            return;
        }
        self.incarnation = Some(incarnation);
        let cancellation = self.grant_cancellation(DescriptorGrantCancellationAction::HostOpen {
            host_session: self.host_session,
            operation: ctx.self_addr(),
        });
        self.finish_open(
            ctx,
            Ok(DescriptorOpenGrant::Stream {
                path: self.path.clone(),
                operation: ctx.self_addr(),
                host_binding,
                incarnation,
                ring,
                role,
                cancellation,
            }),
            false,
        );
    }

    fn reject_finish(&self, ctx: &Ctx<'_>, reply_to: Option<ActorAddress>, error: DataPlaneError) {
        if let Some(reply_to) = reply_to {
            let _ = ctx.send(reply_to, Err::<(), _>(error));
        }
    }

    fn seal_requested(
        &mut self,
        ctx: &Ctx<'_>,
        reply_to: Option<ActorAddress>,
        lease: BlobLease,
        metadata: BlobMetadata,
    ) {
        if self.state != WriteOperationState::Filling {
            self.reject_finish(
                ctx,
                reply_to,
                DataPlaneError::Blob(crate::protocol::BlobFailure::AlreadyFinished),
            );
            return;
        }
        let Some((binding, expected_lease, expected_metadata)) = &self.grant else {
            self.reject_finish(ctx, reply_to, DataPlaneError::OperationCancelled);
            return;
        };
        if lease != *expected_lease || metadata != *expected_metadata {
            self.reject_finish(
                ctx,
                reply_to,
                DataPlaneError::Blob(crate::protocol::BlobFailure::InvalidLease),
            );
            return;
        }
        let binding = *binding;
        self.finish_reply = reply_to;
        self.enter_state(
            ctx,
            WriteOperationState::Sealing,
            Reason::CloseRequested,
            None,
        );
        if let Err(error) = ctx.send(
            self.host_session,
            HostSessionIn::SealWriteBlob {
                binding,
                operation: ctx.self_addr(),
                lease,
                metadata,
            },
        ) {
            self.fail(ctx, DataPlaneError::SessionFailed(error.to_string()));
        }
    }

    fn abort_requested(&mut self, ctx: &Ctx<'_>, reply_to: Option<ActorAddress>, lease: BlobLease) {
        if !matches!(
            self.state,
            WriteOperationState::Filling | WriteOperationState::Sealing
        ) {
            self.reject_finish(
                ctx,
                reply_to,
                DataPlaneError::Blob(crate::protocol::BlobFailure::AlreadyFinished),
            );
            return;
        }
        let Some((binding, expected_lease, _)) = &self.grant else {
            self.reject_finish(ctx, reply_to, DataPlaneError::OperationCancelled);
            return;
        };
        if lease != *expected_lease {
            self.reject_finish(
                ctx,
                reply_to,
                DataPlaneError::Blob(crate::protocol::BlobFailure::InvalidLease),
            );
            return;
        }
        let binding = *binding;
        // Aborting may supersede a seal, but it must not discard its waiter.
        if let Some(seal_reply) = self.finish_reply.take()
            && Some(seal_reply) != reply_to
        {
            let _ = ctx.send(seal_reply, Err::<(), _>(DataPlaneError::OperationCancelled));
        }
        self.finish_reply = reply_to;
        self.enter_state(ctx, WriteOperationState::Aborting, Reason::Cancelled, None);
        if let Err(error) = ctx.send(
            self.host_session,
            HostSessionIn::AbortWriteBlob {
                binding,
                operation: ctx.self_addr(),
                lease_id: lease.lease_id,
                generation: lease.generation,
            },
        ) {
            self.fail(ctx, DataPlaneError::SessionFailed(error.to_string()));
        }
    }

    fn write_published(&mut self, ctx: &Ctx<'_>) {
        if self.state == WriteOperationState::Sealing {
            self.finish_write(ctx, Ok(()));
        }
    }

    fn write_aborted(&mut self, ctx: &Ctx<'_>) {
        if self.state == WriteOperationState::Aborting {
            self.finish_write(ctx, Ok(()));
        }
    }
}

impl ActorInterface for DescriptorOpenOperationActor {
    type Incoming = ChildOperationIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        ctx.watch(self.child_session);
        self.enter_state(ctx, WriteOperationState::Opening, Reason::Started, None);
        if ctx
            .send(
                self.host_session,
                HostSessionIn::Open {
                    path: self.path.clone(),
                    options: self.options.clone(),
                    policy: self.policy,
                    child_session: self.child_session,
                    operation: ctx.self_addr(),
                },
            )
            .is_err()
        {
            self.fail(
                ctx,
                DataPlaneError::SessionFailed("send descriptor open to host session".to_owned()),
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ChildOperationIn) {
        match message {
            ChildOperationIn::ReadOpened {
                host_binding,
                lease,
                metadata,
            } => self.read_opened(ctx, host_binding, lease, metadata),
            ChildOperationIn::WriteOpened {
                host_binding,
                lease,
                metadata,
            } => self.write_opened(ctx, host_binding, lease, metadata),
            ChildOperationIn::StreamOpened {
                host_binding,
                incarnation,
                ring,
                role,
            } => self.stream_opened(ctx, host_binding, incarnation, ring, role),
            ChildOperationIn::SealRequested {
                reply_to,
                lease,
                metadata,
            } => self.seal_requested(ctx, reply_to, lease, metadata),
            ChildOperationIn::AbortRequested { reply_to, lease } => {
                self.abort_requested(ctx, reply_to, lease)
            }
            ChildOperationIn::WritePublished => self.write_published(ctx),
            ChildOperationIn::WriteAborted => self.write_aborted(ctx),
            ChildOperationIn::Failed(error) => self.fail(ctx, error),
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, _exited: ActorExited) {
        self.fail(ctx, DataPlaneError::OperationCancelled);
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        self.fail(ctx, DataPlaneError::OperationCancelled);
    }
}

pub fn parse_actor_address(encoded: &str) -> Result<ActorAddress, DataPlaneError> {
    if encoded.len() != 64 {
        return Err(DataPlaneError::SessionFailed(
            "data-plane actor address must contain 64 hexadecimal digits".to_owned(),
        ));
    }
    let mut bytes = [0_u8; 32];
    for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
        let text = std::str::from_utf8(pair).map_err(|_| {
            DataPlaneError::SessionFailed("data-plane actor address is not hexadecimal".to_owned())
        })?;
        bytes[index] = u8::from_str_radix(text, 16).map_err(|_| {
            DataPlaneError::SessionFailed("data-plane actor address is not hexadecimal".to_owned())
        })?;
    }
    Ok(ActorAddress(bytes))
}

#[cfg(test)]
mod child_operation_lifecycle_tests {
    use super::*;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{Engine, SteppingBackend};

    fn settle(backend: &SteppingBackend) {
        for _ in 0..32 {
            backend.step();
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn stream_reader_rechecks_terminal_ring_after_binding_retirement() {
        use crate::arena::{ArenaConfig, ArenaManager, NodeId};
        use crate::byte_ring::{self, ByteRingSpec};
        use futures_lite::future;

        for (termination, buffered) in [
            (PeerTermination::Closed, false),
            (PeerTermination::PathReplaced, false),
            (PeerTermination::Closed, true),
            (PeerTermination::PathReplaced, true),
        ] {
            let parts = RuntimeParts::new(swactor::config::RuntimeConfig {
                worker_count: 1,
                ..Default::default()
            });
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let _engine = Engine::new(parts, backend).unwrap();
            let session = runtime.new_inbox::<ChildSessionIn>().unwrap();
            let mut allocator = ArenaManager::boot(ArenaConfig {
                node_id: NodeId(1),
                reservation_ceiling: 4096,
                base_alignment: 64,
            })
            .unwrap();
            let prepared = crate::bootstrap::prepare_arena(
                &mut allocator,
                crate::bootstrap::BootstrapSpec {
                    arena_generation: 1,
                    alignment: 64,
                },
            )
            .unwrap();
            let ring = byte_ring::install(
                &mut allocator,
                ByteRingSpec {
                    capacity: 128,
                    generation: 1,
                    alignment: 64,
                    request_id: 1,
                },
            )
            .unwrap();
            let (arena, _) = MappedArena::map(prepared.arena_fd).unwrap();
            let path = DataPath::parse("/streams/retired-binding").unwrap();
            let mut reader = StreamReader {
                runtime: runtime.clone(),
                child_session: *session.addr(),
                operation: ActorAddress::new_random(),
                host_binding: ActorAddress::new_random(),
                incarnation: StreamIncarnation {
                    authority_epoch: 1,
                    revision: 1,
                },
                path: path.clone(),
                endpoint: attach_mapped(&arena, ring, Role::Consumer).unwrap(),
                terminal: None,
                pending_record: None,
            };
            future::block_on(async {
                let mut reading = Box::pin(reader.read());
                assert!(future::poll_once(reading.as_mut()).await.is_none());
                let Some(ChildSessionIn::StreamControl {
                    message: HostStreamIn::WaitData { reply_to },
                    ..
                }) = session.try_recv()
                else {
                    panic!("empty live ring must register a data wait");
                };

                // Retirement can overtake the wait between its ring check and
                // host delivery. The session has forgotten the binding's exact
                // error, but the ring retains its authoritative terminal fence.
                if buffered {
                    let mut writer = attach_mapped(&arena, ring, Role::Producer).unwrap();
                    writer.send_record(RecordKind::Data, b"last").unwrap();
                }
                byte_ring::mark_peer_terminated_mapped(&arena, ring, termination).unwrap();
                runtime
                    .send_to(
                        reply_to,
                        ChildStreamIn::Wake(Err(DataPlaneError::StreamClosed)),
                    )
                    .unwrap();
                let mut result = reading.await;
                if buffered {
                    assert_eq!(result, Ok(Some(b"last".to_vec())));
                    result = reader.read().await;
                }
                let expected = match termination {
                    PeerTermination::Closed => DataPlaneError::PeerLost,
                    PeerTermination::PathReplaced => DataPlaneError::PathReplaced(path.clone()),
                };
                assert_eq!(result, Err(expected));
            });
        }
    }

    #[test]
    fn invalid_seal_retains_owner_and_abort_resolves_superseded_seal_once() {
        let parts = RuntimeParts::new(swactor::config::RuntimeConfig {
            worker_count: 1,
            ..Default::default()
        })
        .with_extension(Arc::new(swactor::std::StdExtension::new()));
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let _engine = Engine::new(parts, backend.clone()).unwrap();
        let host = runtime.new_inbox::<HostSessionIn>().unwrap();
        let session = runtime.new_inbox::<ChildSessionIn>().unwrap();
        let opened = runtime
            .new_inbox::<Result<DescriptorOpenGrant, DataPlaneError>>()
            .unwrap();
        let operation = runtime
            .spawn(DescriptorOpenOperationActor {
                runtime: runtime.clone(),
                host_session: *host.addr(),
                child_session: *session.addr(),
                path: DataPath::parse("/cases/lease").unwrap(),
                options: OpenOptions::staged_blob(4),
                policy: OpenPolicy::Ordinary,
                open_reply: Some(*opened.addr()),
                finish_reply: None,
                grant: None,
                state: WriteOperationState::Opening,
                incarnation: None,
                transitions: None,
            })
            .unwrap();
        settle(&backend);
        assert!(
            matches!(host.try_recv(), Some(HostSessionIn::Open { operation: found, .. }) if found == operation)
        );
        let lease = BlobLease {
            lease_id: crate::ids::BlobLeaseId(1),
            offset: 0,
            length: 4,
            generation: 19,
            access: crate::blob::BlobAccess::Writable,
        };
        let metadata = BlobMetadata {
            length: 4,
            digest: None,
        };
        let binding = ActorAddress::new_random();
        runtime
            .send_to(
                operation,
                ChildOperationIn::WriteOpened {
                    host_binding: binding,
                    lease,
                    metadata: metadata.clone(),
                },
            )
            .unwrap();
        settle(&backend);
        let Some(Ok(DescriptorOpenGrant::WriteBlob { cancellation, .. })) = opened.try_recv()
        else {
            panic!("writable grant must resolve its caller");
        };
        cancellation.disarm();
        let invalid = runtime.new_inbox::<Result<(), DataPlaneError>>().unwrap();
        runtime
            .send_to(
                operation,
                ChildOperationIn::SealRequested {
                    reply_to: Some(*invalid.addr()),
                    lease: BlobLease {
                        generation: 18,
                        ..lease
                    },
                    metadata: metadata.clone(),
                },
            )
            .unwrap();
        settle(&backend);
        assert_eq!(
            invalid.try_recv(),
            Some(Err(DataPlaneError::Blob(
                crate::protocol::BlobFailure::InvalidLease
            )))
        );
        assert!(
            host.try_recv().is_none(),
            "stale lease must not cancel the valid owner's host binding"
        );
        assert!(
            runtime
                .stats()
                .actors
                .iter()
                .any(|(actor, _)| *actor == operation)
        );
        let sealed = runtime.new_inbox::<Result<(), DataPlaneError>>().unwrap();
        runtime
            .send_to(
                operation,
                ChildOperationIn::SealRequested {
                    reply_to: Some(*sealed.addr()),
                    lease,
                    metadata,
                },
            )
            .unwrap();
        settle(&backend);
        assert!(
            matches!(host.try_recv(), Some(HostSessionIn::SealWriteBlob { binding: found, .. }) if found == binding)
        );
        assert!(sealed.try_recv().is_none());
        let aborted = runtime.new_inbox::<Result<(), DataPlaneError>>().unwrap();
        runtime
            .send_to(
                operation,
                ChildOperationIn::AbortRequested {
                    reply_to: Some(*aborted.addr()),
                    lease,
                },
            )
            .unwrap();
        settle(&backend);
        assert_eq!(
            sealed.try_recv(),
            Some(Err(DataPlaneError::OperationCancelled))
        );
        assert!(
            matches!(host.try_recv(), Some(HostSessionIn::AbortWriteBlob { binding: found, .. }) if found == binding)
        );
        runtime
            .send_to(operation, ChildOperationIn::WritePublished)
            .unwrap();
        settle(&backend);
        assert!(
            aborted.try_recv().is_none(),
            "stale publish must not acknowledge abort"
        );
        runtime
            .send_to(operation, ChildOperationIn::WriteAborted)
            .unwrap();
        runtime
            .send_to(operation, ChildOperationIn::WriteAborted)
            .unwrap();
        settle(&backend);
        assert_eq!(aborted.try_recv(), Some(Ok(())));
        assert!(aborted.try_recv().is_none());
        assert!(sealed.try_recv().is_none());
        assert!(invalid.try_recv().is_none());
        assert!(
            matches!(session.try_recv(), Some(ChildSessionIn::OperationDone { operation: found }) if found == operation)
        );
        assert!(session.try_recv().is_none());
        assert!(runtime.stats().actors.is_empty());
    }

    #[test]
    fn operation_stop_before_grant_resolves_caller_and_returns_host_ownership_once() {
        let parts = RuntimeParts::new(swactor::config::RuntimeConfig {
            worker_count: 1,
            ..Default::default()
        })
        .with_extension(Arc::new(swactor::std::StdExtension::new()));
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let _engine = Engine::new(parts, backend.clone()).unwrap();
        let host = runtime.new_inbox::<HostSessionIn>().unwrap();
        let session = runtime.new_inbox::<ChildSessionIn>().unwrap();
        let opened = runtime
            .new_inbox::<Result<DescriptorOpenGrant, DataPlaneError>>()
            .unwrap();
        let operation = runtime
            .spawn(DescriptorOpenOperationActor {
                runtime: runtime.clone(),
                host_session: *host.addr(),
                child_session: *session.addr(),
                path: DataPath::parse("/cases/cancel").unwrap(),
                options: OpenOptions::read_only(),
                policy: OpenPolicy::Ordinary,
                open_reply: Some(*opened.addr()),
                finish_reply: None,
                grant: None,
                state: WriteOperationState::Opening,
                incarnation: None,
                transitions: None,
            })
            .unwrap();
        settle(&backend);
        assert!(matches!(host.try_recv(), Some(HostSessionIn::Open { .. })));
        runtime.stop_actor(operation).unwrap();
        settle(&backend);
        assert!(matches!(
            opened.try_recv(),
            Some(Err(DataPlaneError::OperationCancelled))
        ));
        assert!(opened.try_recv().is_none());
        assert!(
            matches!(host.try_recv(), Some(HostSessionIn::CancelOpen { operation: found }) if found == operation)
        );
        assert!(host.try_recv().is_none());
        assert!(runtime.stats().actors.is_empty());
    }
}
