//! Child-side data-plane session, per-operation actors, and native API.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::os::fd::OwnedFd;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Runtime};
use swactor_engine::{ActorCompletion, EngineHandle};

use crate::blob::{
    Blob, BlobLease, BlobMetadata, BlobView, LeaseReleaser, WritableArenaView, WritableBlobLease,
    WritableViewObserver,
};
use crate::byte_ring::{
    Endpoint, FlowError, RecordCursor, RecordKind, RingHandle, Role, attach_mapped,
};
use crate::mapped_arena::MappedArena;
use crate::path::DataPath;
pub use crate::protocol::{
    AccessMode, BlobAllocation, DataPlaneError, DescriptorCapabilities, DescriptorKind, Errno,
    OpenOptions,
};
use crate::protocol::{ChildSessionIn, HostSessionIn, HostStreamIn, JobCapability, OpenPolicy};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChildSessionState {
    Attaching,
    Running,
    Closing,
    Closed,
}

pub struct DataPlaneBootstrap {
    pub arena: Arc<MappedArena>,
    pub data_plane: DataPlane,
}

pub struct AttachDeadline {
    pub engine: EngineHandle,
    pub timeout: Duration,
}

impl DataPlaneBootstrap {
    pub async fn attach(
        arena_fd: OwnedFd,
        runtime: Runtime,
        host_session: ActorAddress,
        job_capability: JobCapability,
    ) -> Result<Self, DataPlaneError> {
        Self::attach_routed(arena_fd, runtime, host_session, job_capability, None).await
    }

    pub fn map_arena(
        arena_fd: OwnedFd,
    ) -> Result<(Arc<MappedArena>, crate::bootstrap::ResolvedBootstrap), DataPlaneError> {
        let (arena, resolved) = MappedArena::map(arena_fd)
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        Ok((Arc::new(arena), resolved))
    }

    pub async fn attach_routed(
        arena_fd: OwnedFd,
        runtime: Runtime,
        host_session: ActorAddress,
        job_capability: JobCapability,
        child_node: Option<[u8; 32]>,
    ) -> Result<Self, DataPlaneError> {
        let (arena, resolved) = Self::map_arena(arena_fd)?;
        Self::attach_mapped(
            arena,
            resolved,
            runtime,
            host_session,
            job_capability,
            child_node,
        )
        .await
    }

    pub async fn attach_mapped(
        arena: Arc<MappedArena>,
        resolved: crate::bootstrap::ResolvedBootstrap,
        runtime: Runtime,
        host_session: ActorAddress,
        job_capability: JobCapability,
        child_node: Option<[u8; 32]>,
    ) -> Result<Self, DataPlaneError> {
        Self::attach_mapped_inner(
            arena,
            resolved,
            runtime,
            host_session,
            job_capability,
            child_node,
            None,
        )
        .await
    }

    pub async fn attach_mapped_with_deadline(
        arena: Arc<MappedArena>,
        resolved: crate::bootstrap::ResolvedBootstrap,
        runtime: Runtime,
        host_session: ActorAddress,
        job_capability: JobCapability,
        child_node: Option<[u8; 32]>,
        deadline: AttachDeadline,
    ) -> Result<Self, DataPlaneError> {
        let sender = runtime.create_sender();
        Self::attach_mapped_inner(
            arena,
            resolved,
            runtime,
            host_session,
            job_capability,
            child_node,
            Some((deadline.engine, sender, deadline.timeout)),
        )
        .await
    }

    async fn attach_mapped_inner(
        arena: Arc<MappedArena>,
        resolved: crate::bootstrap::ResolvedBootstrap,
        runtime: Runtime,
        host_session: ActorAddress,
        job_capability: JobCapability,
        child_node: Option<[u8; 32]>,
        deadline: Option<(EngineHandle, ExternalSender, Duration)>,
    ) -> Result<Self, DataPlaneError> {
        let attached = runtime
            .new_inbox::<Result<u64, DataPlaneError>>()
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        let attach_reply = *attached.addr();
        let child = runtime
            .spawn(ChildDataPlaneSessionActor {
                runtime: runtime.clone(),
                host_session,
                arena_generation: resolved.arena_generation,
                job_capability,
                child_node,
                session_generation: None,
                attach_reply: Some(attach_reply),
                operations: HashSet::new(),
                open_operations: HashMap::new(),
                state: ChildSessionState::Attaching,
                stream_operations: HashMap::new(),
                pending_blob_releases: 0,
                deferred_blob_opens: VecDeque::new(),
            })
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
        if let Some((engine, sender, timeout)) = deadline {
            engine.send_after(timeout, sender, child, ChildSessionIn::AttachmentDeadline);
        }

        attached.recv().await?;
        Ok(Self {
            arena: arena.clone(),
            data_plane: DataPlane {
                runtime,
                child_session: child,
                arena,
            },
        })
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
        operation: ActorAddress,
        host_binding: ActorAddress,
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

    pub fn as_ref(&self) -> &[u8] {
        match self {
            Self::ReadOnly(view) => view.as_ref(),
            Self::WritableReadOnly(view) | Self::Writable(view) => view.as_ref(),
        }
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
                operation,
                host_binding,
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
                        endpoint,
                        terminal: None,
                        pending_record: None,
                    }),
                    Role::Producer => Descriptor::write_stream(StreamWriter {
                        runtime: self.runtime.clone(),
                        child_session: self.child_session,
                        operation,
                        host_binding,
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
            .send_to(self.child_session, ChildSessionIn::Close)
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))
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
                host_binding: None,
                endpoint: None,
                pending_result: None,
                finished: false,
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
}

#[derive(Clone)]
pub(crate) enum ChildStreamIn {
    Opened(Result<StreamOpenGrant, DataPlaneError>),
    Wake(Result<(), DataPlaneError>),
}

pub struct StreamWriter {
    runtime: Runtime,
    child_session: ActorAddress,
    operation: ActorAddress,
    host_binding: ActorAddress,
    endpoint: Endpoint,
    closed: bool,
}

impl StreamWriter {
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
                            ChildStreamIn::Opened(_) => {
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
            self.send_control(HostStreamIn::WaitCapacity {
                reply_to: *inbox.addr(),
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
                ChildStreamIn::Opened(_) => {
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
            if self.endpoint.peer_terminated().map_err(|error| {
                DataPlaneError::StreamFault(format!(
                    "observe stream peer terminal state: {error:?}"
                ))
            })? {
                return Err(DataPlaneError::BrokenPipe);
            }
            let available = self.endpoint.writable_payload_capacity().map_err(|error| {
                DataPlaneError::StreamFault(format!("observe writable stream capacity: {error:?}"))
            })?;
            if available != 0 {
                let count = bytes
                    .len()
                    .min(usize::try_from(available).unwrap_or(usize::MAX));
                let mut record = self
                    .endpoint
                    .reserve_record(RecordKind::Data, count as u64)
                    .map_err(|error| {
                        DataPlaneError::StreamFault(format!(
                            "reserve partial stream record: {error:?}"
                        ))
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
                DataPlaneError::StreamFault(format!("recheck writable stream capacity: {error:?}"))
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
                ChildStreamIn::Opened(_) => {
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
            ChildStreamIn::Opened(_) => Err(DataPlaneError::StreamFault(
                "received stream-open result while aborting writer".to_owned(),
            )),
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
    endpoint: Endpoint,
    terminal: Option<StreamReadTerminal>,
    pending_record: Option<(RecordCursor, u64)>,
}

impl StreamReader {
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
    async fn close_clean(&mut self) -> Result<(), DataPlaneError> {
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
            ChildStreamIn::Opened(_) => Err(DataPlaneError::StreamFault(
                "received stream-open result while closing reader".to_owned(),
            )),
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
            .map_err(|error| {
                DataPlaneError::StreamFault(format!("consume stream ring: {error:?}"))
            })?;
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
            if self.pending_record.is_none() {
                if let Some(cursor) = self.endpoint.record_cursor().map_err(|error| {
                    DataPlaneError::StreamFault(format!("read stream ring: {error:?}"))
                })? {
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
                            let close_result = self.close_clean().await;
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
                .map_err(|error| {
                    DataPlaneError::StreamFault(format!("recheck stream ring: {error:?}"))
                })?
                .is_some()
            {
                continue;
            }
            match inbox.recv().await {
                ChildStreamIn::Wake(Ok(())) => {}
                ChildStreamIn::Wake(Err(error)) => {
                    self.terminal = Some(StreamReadTerminal::Error(error.clone()));
                    return Err(error);
                }
                ChildStreamIn::Opened(_) => {
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
        let result = self.close_clean().await;
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
            ChildStreamIn::Opened(_) => Err(DataPlaneError::StreamFault(
                "received stream-open result while aborting reader".to_owned(),
            )),
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
    host_binding: Option<ActorAddress>,
    endpoint: Option<Endpoint>,
    pending_result: Option<Result<(), DataPlaneError>>,
    finished: bool,
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
    fn send_control(&self, ctx: &Ctx<'_>, binding: ActorAddress, message: HostStreamIn) {
        let _ = ctx.send(
            self.child_session,
            ChildSessionIn::StreamControl { binding, message },
        );
    }

    fn complete_now(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.finished {
            return;
        }
        self.finished = true;
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
        if self.finished || self.pending_result.is_some() {
            return;
        }
        if clean && let Some(host_binding) = self.host_binding {
            self.pending_result = Some(result);
            self.send_control(
                ctx,
                host_binding,
                HostStreamIn::Close {
                    clean: true,
                    reply_to: Some(ctx.self_addr()),
                },
            );
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
        loop {
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
        let _ = ctx.send(
            self.child_session,
            ChildSessionIn::OpenReadStream {
                path: self.path.clone(),
                reply_to: ctx.self_addr(),
                replace: false,
            },
        );
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ChildStreamIn) {
        match message {
            ChildStreamIn::Opened(Ok(grant)) => {
                match attach_mapped(&self.arena, grant.ring, Role::Consumer) {
                    Ok(endpoint) => {
                        self.operation = Some(grant.operation);
                        self.host_binding = Some(grant.host_binding);
                        self.endpoint = Some(endpoint);
                        self.drain(ctx);
                    }
                    Err(error) => self.finish(
                        ctx,
                        Err(DataPlaneError::StreamFault(format!(
                            "attach stream collector: {error:?}"
                        ))),
                        false,
                    ),
                }
            }
            ChildStreamIn::Opened(Err(error)) => {
                self.finish(ctx, Err(error), false);
            }
            ChildStreamIn::Wake(result) => {
                if let Some(pending) = self.pending_result.take() {
                    let completed = match result {
                        Ok(()) => pending,
                        Err(error) => Err(error),
                    };
                    self.complete_now(ctx, completed);
                } else {
                    match result {
                        Ok(()) => self.drain(ctx),
                        Err(error) => self.finish(ctx, Err(error), false),
                    }
                }
            }
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.finished {
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
            let _ = self
                .completion
                .complete(Err(DataPlaneError::OperationCancelled));
        }
    }
}

pub struct ChildDataPlaneSessionActor {
    runtime: Runtime,
    host_session: ActorAddress,
    arena_generation: u64,
    job_capability: JobCapability,
    child_node: Option<[u8; 32]>,
    session_generation: Option<u64>,
    attach_reply: Option<ActorAddress>,
    operations: HashSet<ActorAddress>,
    open_operations: HashMap<ActorAddress, ActorAddress>,
    state: ChildSessionState,
    stream_operations: HashMap<ActorAddress, ActorAddress>,
    pending_blob_releases: usize,
    deferred_blob_opens: VecDeque<ChildSessionIn>,
}

impl ChildDataPlaneSessionActor {
    pub fn state(&self) -> ChildSessionState {
        self.state
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
                Err::<StreamOpenGrant, _>(DataPlaneError::SessionNotRunning),
            );
            return;
        }
        let actor = StreamOpenOperationActor {
            host_session: self.host_session,
            child_session: ctx.self_addr(),
            path,
            role,
            replace,
            reply_to,
            replied: false,
        };
        match ctx.spawn(actor) {
            Ok(operation) => {
                self.operations.insert(operation);
                self.stream_operations.insert(reply_to, operation);
            }
            Err(error) => {
                let _ = ctx.send(
                    reply_to,
                    Err::<StreamOpenGrant, _>(DataPlaneError::SessionFailed(error.to_string())),
                );
            }
        }
    }
}

impl ActorInterface for ChildDataPlaneSessionActor {
    type Incoming = ChildSessionIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if ctx
            .send(
                self.host_session,
                HostSessionIn::Attach {
                    child_session: ctx.self_addr(),
                    arena_generation: self.arena_generation,
                    job_capability: self.job_capability,
                    child_node: self.child_node,
                },
            )
            .is_err()
        {
            self.state = ChildSessionState::Closed;
            if let Some(reply_to) = self.attach_reply.take() {
                let _ = ctx.send(
                    reply_to,
                    Err::<u64, _>(DataPlaneError::SessionFailed(
                        "route to host data-plane session is unavailable".to_owned(),
                    )),
                );
            }
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: ChildSessionIn) {
        match message {
            ChildSessionIn::Attached { session_generation }
                if self.state == ChildSessionState::Attaching =>
            {
                self.session_generation = Some(session_generation);
                self.state = ChildSessionState::Running;
                if let Some(reply_to) = self.attach_reply.take() {
                    let _ = ctx.send(reply_to, Ok::<_, DataPlaneError>(session_generation));
                }
            }
            ChildSessionIn::AttachmentFailed { error }
                if self.state == ChildSessionState::Attaching =>
            {
                self.state = ChildSessionState::Closed;
                if let Some(reply_to) = self.attach_reply.take() {
                    let _ = ctx.send(reply_to, Err::<u64, _>(error));
                }
            }
            ChildSessionIn::AttachmentDeadline if self.state == ChildSessionState::Attaching => {
                self.state = ChildSessionState::Closed;
                if let Some(reply_to) = self.attach_reply.take() {
                    let _ = ctx.send(
                        reply_to,
                        Err::<u64, _>(DataPlaneError::SessionFailed(
                            "data-plane attachment deadline elapsed".to_owned(),
                        )),
                    );
                }
                ctx.stop_self();
            }
            ChildSessionIn::Open {
                path,
                options,
                policy,
                reply_to,
            } => {
                if self.pending_blob_releases != 0 {
                    self.deferred_blob_opens.push_back(ChildSessionIn::Open {
                        path,
                        options,
                        policy,
                        reply_to,
                    });
                    return;
                }
                if self.state != ChildSessionState::Running {
                    let _ = ctx.send(
                        reply_to,
                        Err::<DescriptorOpenGrant, _>(DataPlaneError::SessionNotRunning),
                    );
                    return;
                }
                let actor = DescriptorOpenOperationActor {
                    runtime: self.runtime.clone(),
                    host_session: self.host_session,
                    child_session: ctx.self_addr(),
                    path,
                    options,
                    policy,
                    open_reply: Some(reply_to),
                    finish_reply: None,
                    grant: None,
                    state: WriteOperationState::Opening,
                    replied: false,
                };
                match ctx.spawn(actor) {
                    Ok(operation) => {
                        self.operations.insert(operation);
                        self.open_operations.insert(reply_to, operation);
                    }
                    Err(error) => {
                        let _ = ctx.send(
                            reply_to,
                            Err::<DescriptorOpenGrant, _>(DataPlaneError::SessionFailed(
                                error.to_string(),
                            )),
                        );
                    }
                }
            }
            ChildSessionIn::CancelOpen { reply_to } => {
                if let Some(operation) = self.open_operations.remove(&reply_to) {
                    self.operations.remove(&operation);
                    let _ = ctx.stop_actor(operation);
                }
            }
            ChildSessionIn::OpenReadStream {
                path,
                reply_to,
                replace,
            } => {
                self.start_stream_open(ctx, path, reply_to, Role::Consumer, replace);
            }
            ChildSessionIn::CancelStream { reply_to } => {
                if let Some(operation) = self.stream_operations.remove(&reply_to) {
                    self.operations.remove(&operation);
                    let _ = ctx.stop_actor(operation);
                }
            }
            ChildSessionIn::StreamWake { reply_to, result } => {
                let _ = ctx.send(reply_to, ChildStreamIn::Wake(result));
            }
            ChildSessionIn::StreamControl { binding, message } => {
                let _ = ctx.send(
                    self.host_session,
                    HostSessionIn::StreamControl { binding, message },
                );
            }
            ChildSessionIn::BlobReleased => {
                self.pending_blob_releases = self.pending_blob_releases.saturating_sub(1);
                if self.pending_blob_releases == 0 {
                    while let Some(deferred) = self.deferred_blob_opens.pop_front() {
                        self.handle(ctx, deferred);
                        if self.pending_blob_releases != 0 {
                            break;
                        }
                    }
                }
            }
            ChildSessionIn::ReleaseBlob {
                binding,
                lease_id,
                generation,
            } => {
                self.pending_blob_releases = self.pending_blob_releases.saturating_add(1);
                let _ = ctx.send(
                    self.host_session,
                    HostSessionIn::ReleaseBlob {
                        binding,
                        lease_id,
                        generation,
                    },
                );
            }
            ChildSessionIn::BlobOpened {
                operation,
                host_binding,
                lease,
                metadata,
            } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(
                        operation,
                        ChildOperationIn::ReadOpened {
                            host_binding,
                            lease,
                            metadata,
                        },
                    );
                }
            }
            ChildSessionIn::WriteBlobOpened {
                operation,
                host_binding,
                lease,
                metadata,
            } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(
                        operation,
                        ChildOperationIn::WriteOpened {
                            host_binding,
                            lease,
                            metadata,
                        },
                    );
                }
            }
            ChildSessionIn::StreamOpened {
                operation,
                host_binding,
                ring,
                role,
            } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(
                        operation,
                        ChildOperationIn::StreamOpened {
                            host_binding,
                            ring,
                            role,
                        },
                    );
                }
            }
            ChildSessionIn::OperationFailed { operation, error } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(operation, ChildOperationIn::Failed(error));
                }
            }
            ChildSessionIn::WritePublished { operation } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(operation, ChildOperationIn::WritePublished);
                }
            }
            ChildSessionIn::WriteAborted { operation } => {
                if self.operations.contains(&operation) {
                    let _ = ctx.send(operation, ChildOperationIn::WriteAborted);
                }
            }
            ChildSessionIn::OperationDone { operation } => {
                self.operations.remove(&operation);
                self.open_operations
                    .retain(|_, open_operation| *open_operation != operation);
                self.stream_operations
                    .retain(|_, stream_operation| *stream_operation != operation);
            }
            ChildSessionIn::Close => {
                if matches!(
                    self.state,
                    ChildSessionState::Closing | ChildSessionState::Closed
                ) {
                    return;
                }
                self.state = ChildSessionState::Closing;
                for operation in self.operations.iter().copied() {
                    let _ = ctx.stop_actor(operation);
                }
                self.open_operations.clear();
                self.stream_operations.clear();
                let _ = ctx.send(self.host_session, HostSessionIn::Close);
                self.state = ChildSessionState::Closed;
            }
            _ => {}
        }
    }
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

struct StreamOpenOperationActor {
    host_session: ActorAddress,
    child_session: ActorAddress,
    path: DataPath,
    role: Role,
    reply_to: ActorAddress,
    replace: bool,
    replied: bool,
}

impl StreamOpenOperationActor {
    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<StreamOpenGrant, DataPlaneError>) {
        self.replied = true;
        let _ = ctx.send(self.reply_to, ChildStreamIn::Opened(result));
        let _ = ctx.send(
            self.child_session,
            ChildSessionIn::OperationDone {
                operation: ctx.self_addr(),
            },
        );
        ctx.stop_self();
    }
}

impl ActorInterface for StreamOpenOperationActor {
    type Incoming = ChildOperationIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
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
                ring,
                role,
            } if role == self.role => self.finish(
                ctx,
                Ok(StreamOpenGrant {
                    operation: ctx.self_addr(),
                    host_binding,
                    ring,
                }),
            ),
            ChildOperationIn::StreamOpened { .. } => self.finish(
                ctx,
                Err(DataPlaneError::StreamFault(
                    "host opened stream with the wrong ring role".to_owned(),
                )),
            ),
            ChildOperationIn::Failed(error) => self.finish(ctx, Err(error)),
            _ => {}
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.replied {
            let _ = ctx.send(
                self.reply_to,
                ChildStreamIn::Opened(Err(DataPlaneError::OperationCancelled)),
            );
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::CancelOpen {
                    operation: ctx.self_addr(),
                },
            );
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
    replied: bool,
}

impl DescriptorOpenOperationActor {
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
        self.replied = true;
        if let Some(reply_to) = self.open_reply.take() {
            let _ = ctx.send(reply_to, result);
        }
        if !keep_alive {
            self.state = WriteOperationState::Finished;
            self.operation_done(ctx);
            ctx.stop_self();
        }
    }

    fn finish_write(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        self.state = WriteOperationState::Finished;
        if let Some(reply_to) = self.finish_reply.take() {
            let _ = ctx.send(reply_to, result);
        }
        self.operation_done(ctx);
        ctx.stop_self();
    }

    fn fail(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        if self.open_reply.is_some() {
            self.finish_open(ctx, Err(error), false);
        } else {
            self.finish_write(ctx, Err(error));
        }
    }
}

impl ActorInterface for DescriptorOpenOperationActor {
    type Incoming = ChildOperationIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
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
            } if self.state == WriteOperationState::Opening => {
                let cancellation = Arc::new(DescriptorGrantCancellation {
                    runtime: self.runtime.clone(),
                    action: DescriptorGrantCancellationAction::HostOpen {
                        host_session: self.host_session,
                        operation: ctx.self_addr(),
                    },
                    armed: AtomicBool::new(true),
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
            ChildOperationIn::WriteOpened {
                host_binding,
                lease,
                metadata,
            } if self.state == WriteOperationState::Opening => {
                self.state = WriteOperationState::Filling;
                self.grant = Some((host_binding, lease, metadata.clone()));
                let cancellation = Arc::new(DescriptorGrantCancellation {
                    runtime: self.runtime.clone(),
                    action: DescriptorGrantCancellationAction::WriteBlob {
                        operation: ctx.self_addr(),
                        lease,
                    },
                    armed: AtomicBool::new(true),
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
            ChildOperationIn::StreamOpened {
                host_binding,
                ring,
                role,
            } if self.state == WriteOperationState::Opening => {
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
                let cancellation = Arc::new(DescriptorGrantCancellation {
                    runtime: self.runtime.clone(),
                    action: DescriptorGrantCancellationAction::HostOpen {
                        host_session: self.host_session,
                        operation: ctx.self_addr(),
                    },
                    armed: AtomicBool::new(true),
                });
                self.finish_open(
                    ctx,
                    Ok(DescriptorOpenGrant::Stream {
                        operation: ctx.self_addr(),
                        host_binding,
                        ring,
                        role,
                        cancellation,
                    }),
                    false,
                );
            }
            ChildOperationIn::SealRequested {
                reply_to,
                lease,
                metadata,
            } if self.state == WriteOperationState::Filling => {
                let Some((host_binding, expected_lease, expected_metadata)) = self.grant.clone()
                else {
                    self.fail(ctx, DataPlaneError::OperationCancelled);
                    return;
                };
                if lease != expected_lease || metadata != expected_metadata {
                    self.fail(
                        ctx,
                        DataPlaneError::Blob(crate::protocol::BlobFailure::InvalidLease),
                    );
                    return;
                }
                self.state = WriteOperationState::Sealing;
                self.finish_reply = reply_to;
                if ctx
                    .send(
                        self.host_session,
                        HostSessionIn::SealWriteBlob {
                            binding: host_binding,
                            operation: ctx.self_addr(),
                            lease,
                            metadata,
                        },
                    )
                    .is_err()
                {
                    self.fail(
                        ctx,
                        DataPlaneError::SessionFailed(
                            "send descriptor blob seal to host session".to_owned(),
                        ),
                    );
                }
            }
            ChildOperationIn::AbortRequested { reply_to, lease }
                if matches!(
                    self.state,
                    WriteOperationState::Filling | WriteOperationState::Sealing
                ) =>
            {
                let Some((host_binding, expected_lease, _)) = self.grant.clone() else {
                    self.fail(ctx, DataPlaneError::OperationCancelled);
                    return;
                };
                if lease != expected_lease {
                    self.fail(
                        ctx,
                        DataPlaneError::Blob(crate::protocol::BlobFailure::InvalidLease),
                    );
                    return;
                }
                self.state = WriteOperationState::Aborting;
                self.finish_reply = reply_to;
                if ctx
                    .send(
                        self.host_session,
                        HostSessionIn::AbortWriteBlob {
                            binding: host_binding,
                            operation: ctx.self_addr(),
                            lease_id: lease.lease_id,
                            generation: lease.generation,
                        },
                    )
                    .is_err()
                {
                    self.fail(
                        ctx,
                        DataPlaneError::SessionFailed(
                            "send descriptor blob abort to host session".to_owned(),
                        ),
                    );
                }
            }
            ChildOperationIn::WritePublished if self.state == WriteOperationState::Sealing => {
                self.finish_write(ctx, Ok(()));
            }
            ChildOperationIn::WriteAborted if self.state == WriteOperationState::Aborting => {
                self.finish_write(ctx, Ok(()));
            }
            ChildOperationIn::Failed(error) => self.fail(ctx, error),
            _ => {}
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.replied {
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::CancelOpen {
                    operation: ctx.self_addr(),
                },
            );
            if let Some(reply_to) = self.open_reply.take() {
                let _ = ctx.send(
                    reply_to,
                    Err::<DescriptorOpenGrant, _>(DataPlaneError::OperationCancelled),
                );
            }
        }
        if let Some(reply_to) = self.finish_reply.take() {
            let _ = ctx.send(reply_to, Err::<(), _>(DataPlaneError::OperationCancelled));
        }
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
