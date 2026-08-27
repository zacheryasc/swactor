//! Typed actor protocol for child and host data-plane sessions.

use std::fmt;

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor_transport::{CodecRegistry, JsonCodec, NetworkMessage};

use crate::blob::{BlobError, BlobLease, BlobMetadata, ContentDigest};
use crate::byte_ring::{RingHandle, Role};
use crate::ids::BlobLeaseId;
use crate::namespace::{
    EntryKind, NamespaceError, NamespaceNode, OperationId, StreamIncarnation, StreamMatch,
};
use crate::path::DataPath;
use crate::stream_transport::{StreamPeerDescriptor, StreamTransportEvent};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionCapability([u8; 32]);

impl SessionCapability {
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn to_hex(self) -> String {
        let mut encoded = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write as _;
            let _ = write!(encoded, "{byte:02x}");
        }
        encoded
    }

    pub fn from_hex(encoded: &str) -> Result<Self, DataPlaneError> {
        if encoded.len() != 64 {
            return Err(DataPlaneError::InvalidCapability);
        }
        let mut bytes = [0_u8; 32];
        for (index, pair) in encoded.as_bytes().chunks_exact(2).enumerate() {
            let text = std::str::from_utf8(pair).map_err(|_| DataPlaneError::InvalidCapability)?;
            bytes[index] =
                u8::from_str_radix(text, 16).map_err(|_| DataPlaneError::InvalidCapability)?;
        }
        Ok(Self(bytes))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccessMode {
    ReadOnly,
    WriteOnly,
    ReadWrite,
}

impl AccessMode {
    pub const fn can_read(self) -> bool {
        matches!(self, Self::ReadOnly | Self::ReadWrite)
    }

    pub const fn can_write(self) -> bool {
        matches!(self, Self::WriteOnly | Self::ReadWrite)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobAllocation {
    pub length: u64,
    pub digest: Option<ContentDigest>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenOptions {
    pub access: AccessMode,
    pub create: bool,
    pub exclusive: bool,
    pub truncate: bool,
    pub nonblocking: bool,
    pub allocation: Option<BlobAllocation>,
}

impl OpenOptions {
    pub const fn read_only() -> Self {
        Self {
            access: AccessMode::ReadOnly,
            create: false,
            exclusive: false,
            truncate: false,
            nonblocking: false,
            allocation: None,
        }
    }

    pub fn staged_blob(length: u64) -> Self {
        Self {
            access: AccessMode::WriteOnly,
            create: true,
            exclusive: false,
            truncate: true,
            nonblocking: false,
            allocation: Some(BlobAllocation {
                length,
                digest: None,
            }),
        }
    }

    pub fn validate(&self) -> Result<(), DataPlaneError> {
        if self.nonblocking {
            return Err(DataPlaneError::Unsupported(
                "O_NONBLOCK is not supported".to_owned(),
            ));
        }
        if self.exclusive && !self.create {
            return Err(DataPlaneError::InvalidArgument(
                "O_EXCL requires O_CREAT".to_owned(),
            ));
        }
        if (self.create || self.truncate) && !self.access.can_write() {
            return Err(DataPlaneError::InvalidArgument(
                "creation and truncation require write access".to_owned(),
            ));
        }
        if self.allocation.is_some() && !(self.access.can_write() && self.truncate) {
            return Err(DataPlaneError::InvalidArgument(
                "blob allocation requires a truncating writable open".to_owned(),
            ));
        }
        Ok(())
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self::read_only()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DescriptorKind {
    Blob,
    Stream,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DescriptorCapabilities(u16);

impl DescriptorCapabilities {
    pub const READ: Self = Self(1 << 0);
    pub const WRITE: Self = Self(1 << 1);
    pub const MAP_HOST: Self = Self(1 << 2);
    pub const MAP_DEVICE: Self = Self(1 << 3);
    pub const SEEK: Self = Self(1 << 4);
    pub const POLL: Self = Self(1 << 5);
    pub const CONTROL: Self = Self(1 << 6);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn contains(self, capability: Self) -> bool {
        self.0 & capability.0 == capability.0
    }

    pub const fn union(self, capability: Self) -> Self {
        Self(self.0 | capability.0)
    }

    pub const fn bits(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Errno {
    Eacces,
    Eagain,
    Ebadf,
    Ebusy,
    Ecanceled,
    Econnreset,
    Eexist,
    Einval,
    Eio,
    Enodev,
    Enoent,
    Enomem,
    Enospc,
    Enotsup,
    Enxio,
    Epipe,
    Estale,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpenPolicy {
    Ordinary,
    EnsureStream { replace: bool },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AttachmentFailure {
    CapabilityRejected,
    ArenaGenerationMismatch { expected: u64, found: u64 },
    DuplicateAttachment,
    SessionClosed,
    RouteRejected(String),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobFailure {
    Bounds,
    StaleGeneration { expected: u64, found: u64 },
    Length { expected: u64, found: u64 },
    Digest,
    Access,
    State { found: u64 },
    InvalidLease,
    ActiveWritableView,
    AlreadyFinished,
}

impl From<BlobError> for BlobFailure {
    fn from(error: BlobError) -> Self {
        match error {
            BlobError::RangeOutOfBounds { .. } | BlobError::UnalignedHeader { .. } => Self::Bounds,
            BlobError::StaleGeneration { expected, found } => {
                Self::StaleGeneration { expected, found }
            }
            BlobError::LengthMismatch { expected, found } => Self::Length { expected, found },
            BlobError::DigestMetadataMismatch | BlobError::DigestMismatch => Self::Digest,
            BlobError::AccessDenied | BlobError::InvalidAccess { .. } => Self::Access,
            BlobError::InvalidState { found } => Self::State { found },
            BlobError::ActiveWritableView => Self::ActiveWritableView,
            BlobError::AlreadyFinished => Self::AlreadyFinished,
            BlobError::BadMagic { .. }
            | BlobError::UnsupportedVersion { .. }
            | BlobError::ReservedBytesNotZero { .. }
            | BlobError::InvalidHostLease => Self::InvalidLease,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataPlaneError {
    InvalidPath(String),
    InvalidArgument(String),
    InvalidCapability,
    Attachment(AttachmentFailure),
    SessionNotRunning,
    SessionFailed(String),
    Unauthorized {
        path: DataPath,
        access: AccessMode,
    },
    PathNotFound(DataPath),
    PathExists(DataPath),
    SourceFailure(String),
    ArenaExhausted,
    Blob(BlobFailure),
    OperationCancelled,
    BadDescriptor,
    Unsupported(String),
    MappingUnsupported,
    Busy(String),
    Stale(String),
    BrokenPipe,
    WrongEntryType {
        path: DataPath,
        expected: EntryKind,
        found: EntryKind,
    },
    PathReplaced(DataPath),
    PeerLost,
    StreamFault(String),
    StreamClosed,
}

impl fmt::Display for DataPlaneError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath(reason) => write!(f, "invalid data path: {reason}"),
            Self::InvalidArgument(reason) => write!(f, "invalid argument: {reason}"),
            Self::InvalidCapability => f.write_str("invalid job capability"),
            Self::Attachment(reason) => write!(f, "data-plane attachment failed: {reason:?}"),
            Self::SessionNotRunning => f.write_str("data-plane session is not running"),
            Self::SessionFailed(reason) => write!(f, "data-plane session failed: {reason}"),
            Self::Unauthorized { path, access } => {
                write!(f, "{access:?} access is not authorized for {path}")
            }
            Self::PathNotFound(path) => write!(f, "data path not found: {path}"),
            Self::PathExists(path) => write!(f, "data path already exists: {path}"),
            Self::SourceFailure(reason) => write!(f, "blob source failed: {reason}"),
            Self::ArenaExhausted => f.write_str("data-plane arena is exhausted"),
            Self::Blob(reason) => write!(f, "blob lease failure: {reason:?}"),
            Self::OperationCancelled => f.write_str("data-plane operation was cancelled"),
            Self::BadDescriptor => f.write_str("bad descriptor"),
            Self::Unsupported(reason) => write!(f, "operation is not supported: {reason}"),
            Self::MappingUnsupported => f.write_str("object does not support mapping"),
            Self::Busy(reason) => write!(f, "resource is busy: {reason}"),
            Self::Stale(reason) => write!(f, "stale capability: {reason}"),
            Self::BrokenPipe => f.write_str("stream peer is closed"),
            Self::WrongEntryType {
                path,
                expected,
                found,
            } => write!(
                f,
                "data path {path} has entry kind {found:?}, expected {expected:?}"
            ),
            Self::PathReplaced(path) => write!(f, "data path was replaced: {path}"),
            Self::PeerLost => f.write_str("stream peer was lost"),
            Self::StreamFault(reason) => write!(f, "stream fault: {reason}"),
            Self::StreamClosed => f.write_str("stream is closed"),
        }
    }
}

impl DataPlaneError {
    pub const fn errno(&self) -> Errno {
        match self {
            Self::InvalidPath(_) | Self::InvalidArgument(_) | Self::InvalidCapability => {
                Errno::Einval
            }
            Self::Attachment(_)
            | Self::SessionNotRunning
            | Self::SessionFailed(_)
            | Self::SourceFailure(_)
            | Self::StreamFault(_) => Errno::Eio,
            Self::Unauthorized { .. } => Errno::Eacces,
            Self::PathNotFound(_) => Errno::Enoent,
            Self::PathExists(_) => Errno::Eexist,
            Self::ArenaExhausted => Errno::Enospc,
            Self::Blob(BlobFailure::Bounds | BlobFailure::Length { .. }) => Errno::Einval,
            Self::Blob(BlobFailure::StaleGeneration { .. }) | Self::Stale(_) => Errno::Estale,
            Self::Blob(BlobFailure::Access) | Self::BadDescriptor | Self::StreamClosed => {
                Errno::Ebadf
            }
            Self::Blob(BlobFailure::ActiveWritableView) | Self::Busy(_) => Errno::Ebusy,
            Self::Blob(
                BlobFailure::Digest
                | BlobFailure::State { .. }
                | BlobFailure::InvalidLease
                | BlobFailure::AlreadyFinished,
            ) => Errno::Eio,
            Self::OperationCancelled => Errno::Ecanceled,
            Self::Unsupported(_) => Errno::Enotsup,
            Self::MappingUnsupported => Errno::Enodev,
            Self::BrokenPipe => Errno::Epipe,
            Self::WrongEntryType { .. } => Errno::Enxio,
            Self::PathReplaced(_) => Errno::Estale,
            Self::PeerLost => Errno::Econnreset,
        }
    }
}

impl std::error::Error for DataPlaneError {}

impl From<BlobError> for DataPlaneError {
    fn from(error: BlobError) -> Self {
        Self::Blob(error.into())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NamespaceOperation {
    Lookup {
        path: DataPath,
    },
    Unlink {
        path: DataPath,
    },
    Rename {
        source: DataPath,
        destination: DataPath,
        replace: bool,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NamespaceOperationResult {
    Node(NamespaceNode),
    Mutation { revision: u64 },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HostSessionIn {
    Attach {
        child_session: ActorAddress,
        arena_generation: u64,
        session_capability: SessionCapability,
        child_node: Option<[u8; 32]>,
    },
    Open {
        path: DataPath,
        options: OpenOptions,
        policy: OpenPolicy,
        child_session: ActorAddress,
        operation: ActorAddress,
    },
    OpenResolved {
        operation: ActorAddress,
        result: Result<NamespaceNode, NamespaceError>,
    },
    BlobReserved {
        operation: ActorAddress,
        path: DataPath,
        reservation: OperationId,
        result: Result<(), NamespaceError>,
    },
    Namespace {
        operation: ActorAddress,
        request: NamespaceOperation,
        child_session: ActorAddress,
    },
    NamespaceResolved {
        operation: ActorAddress,
        child_session: ActorAddress,
        result: Result<NamespaceOperationResult, DataPlaneError>,
    },
    CancelNamespace {
        operation: ActorAddress,
    },
    CancelOpen {
        operation: ActorAddress,
    },
    StreamControl {
        binding: ActorAddress,
        message: HostStreamIn,
    },
    ReleaseBlob {
        binding: ActorAddress,
        lease_id: BlobLeaseId,
        generation: u64,
    },
    SealWriteBlob {
        binding: ActorAddress,
        operation: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    AbortWriteBlob {
        binding: ActorAddress,
        operation: ActorAddress,
        lease_id: BlobLeaseId,
        generation: u64,
    },
    BindingFaulted {
        binding: ActorAddress,
        operation: ActorAddress,
        error: DataPlaneError,
    },
    BindingDone {
        binding: ActorAddress,
    },
    BindingDetached {
        binding: ActorAddress,
    },
    ConfigureExecution {
        execution_id: String,
        reply_to: ActorAddress,
    },
    Revoke,
    Close {
        reply_to: Option<ActorAddress>,
    },
}

impl NetworkMessage for HostSessionIn {
    fn type_tag() -> &'static str {
        "data-plane.host-session.v1"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ChildSessionIn {
    Attached {
        session_generation: u64,
    },
    AttachmentFailed {
        error: DataPlaneError,
    },
    Open {
        path: DataPath,
        options: OpenOptions,
        policy: OpenPolicy,
        reply_to: ActorAddress,
    },
    Namespace {
        request: NamespaceOperation,
        reply_to: ActorAddress,
    },
    CancelNamespace {
        reply_to: ActorAddress,
    },
    NamespaceResolved {
        reply_to: ActorAddress,
        result: Result<NamespaceOperationResult, DataPlaneError>,
    },
    CancelOpen {
        reply_to: ActorAddress,
    },
    OpenReadStream {
        path: DataPath,
        reply_to: ActorAddress,
        replace: bool,
    },
    CancelStream {
        reply_to: ActorAddress,
    },
    StreamWake {
        reply_to: ActorAddress,
        result: Result<(), DataPlaneError>,
    },
    StreamControl {
        binding: ActorAddress,
        message: HostStreamIn,
    },
    ReleaseBlob {
        binding: ActorAddress,
        lease_id: BlobLeaseId,
        generation: u64,
    },
    BlobReleased,
    BlobOpened {
        operation: ActorAddress,
        host_binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    WriteBlobOpened {
        operation: ActorAddress,
        host_binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    StreamOpened {
        operation: ActorAddress,
        host_binding: ActorAddress,
        ring: RingHandle,
        role: Role,
    },
    OperationFailed {
        operation: ActorAddress,
        error: DataPlaneError,
    },
    WritePublished {
        operation: ActorAddress,
    },
    WriteAborted {
        operation: ActorAddress,
    },
    OperationDone {
        operation: ActorAddress,
    },
    Close,
}

impl NetworkMessage for ChildSessionIn {
    fn type_tag() -> &'static str {
        "data-plane.child-session.v1"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HostStreamIn {
    NamespaceMatched(Result<StreamMatch, NamespaceError>),
    Allocated(Result<RingHandle, DataPlaneError>),
    PeerOffer {
        incarnation: StreamIncarnation,
        descriptor: StreamPeerDescriptor,
    },
    PeerOfferAck {
        incarnation: StreamIncarnation,
    },
    PeerOfferRetry {
        incarnation: StreamIncarnation,
    },
    Transport(StreamTransportEvent),
    DataAvailable,
    CapacityAvailable,
    WaitData {
        reply_to: ActorAddress,
    },
    WaitCapacity {
        reply_to: ActorAddress,
    },
    Close {
        clean: bool,
        reply_to: Option<ActorAddress>,
    },
    PeerTerminated {
        incarnation: StreamIncarnation,
        error: DataPlaneError,
        reply_to: Option<ActorAddress>,
    },
    PeerTerminationAck {
        incarnation: StreamIncarnation,
    },
    PeerTerminationRetry {
        incarnation: StreamIncarnation,
    },
    ReleaseComplete(Result<(), DataPlaneError>),
}

impl NetworkMessage for HostStreamIn {
    fn type_tag() -> &'static str {
        "data-plane.host-stream.v1"
    }
}

pub fn register_data_plane_codecs(registry: &mut CodecRegistry) {
    registry
        .register::<HostSessionIn, _>(JsonCodec::default())
        .expect("unique codec registration");
    registry
        .register::<ChildSessionIn, _>(JsonCodec::default())
        .expect("unique codec registration");
    registry
        .register::<HostStreamIn, _>(JsonCodec::default())
        .expect("unique codec registration");
    crate::namespace::register_namespace_codecs(registry);
    crate::blob_transfer::register_blob_transfer_codecs(registry);
    crate::source::register_blob_source_codecs(registry);
}
