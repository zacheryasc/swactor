//! Typed actor protocol for child and host data-plane sessions.

use std::fmt;

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor_transport::{CodecRegistry, JsonCodec, NetworkMessage};

use crate::blob::{BlobError, BlobLease, BlobMetadata};
use crate::byte_ring::{RingHandle, Role};
use crate::ids::BlobLeaseId;
use crate::namespace::{EntryKind, NamespaceError, StreamIncarnation, StreamMatch};
use crate::path::DataPath;
use crate::stream_transport::{StreamPeerDescriptor, StreamTransportEvent};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobCapability([u8; 32]);

impl JobCapability {
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
pub enum DataOperation {
    ReadBlob,
    WriteBlob,
    ReadStream,
    WriteStream,
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
    InvalidCapability,
    Attachment(AttachmentFailure),
    SessionNotRunning,
    SessionFailed(String),
    Unauthorized {
        path: DataPath,
        operation: DataOperation,
    },
    PathNotFound(DataPath),
    SourceFailure(String),
    ArenaExhausted,
    Blob(BlobFailure),
    OperationCancelled,
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
            Self::InvalidCapability => f.write_str("invalid job capability"),
            Self::Attachment(reason) => write!(f, "data-plane attachment failed: {reason:?}"),
            Self::SessionNotRunning => f.write_str("data-plane session is not running"),
            Self::SessionFailed(reason) => write!(f, "data-plane session failed: {reason}"),
            Self::Unauthorized { path, operation } => {
                write!(f, "{operation:?} is not authorized for {path}")
            }
            Self::PathNotFound(path) => write!(f, "data path not found: {path}"),
            Self::SourceFailure(reason) => write!(f, "blob source failed: {reason}"),
            Self::ArenaExhausted => f.write_str("data-plane arena is exhausted"),
            Self::Blob(reason) => write!(f, "blob lease failure: {reason:?}"),
            Self::OperationCancelled => f.write_str("data-plane operation was cancelled"),
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

impl std::error::Error for DataPlaneError {}

impl From<BlobError> for DataPlaneError {
    fn from(error: BlobError) -> Self {
        Self::Blob(error.into())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum HostSessionIn {
    Attach {
        child_session: ActorAddress,
        arena_generation: u64,
        job_capability: JobCapability,
        child_node: Option<[u8; 32]>,
    },
    OpenReadBlob {
        path: DataPath,
        child_session: ActorAddress,
        operation: ActorAddress,
    },
    CancelReadBlob {
        operation: ActorAddress,
    },
    OpenWriteBlob {
        path: DataPath,
        length: u64,
        child_session: ActorAddress,
        operation: ActorAddress,
    },
    OpenReadStream {
        path: DataPath,
        child_session: ActorAddress,
        operation: ActorAddress,
        replace: bool,
    },
    OpenWriteStream {
        path: DataPath,
        child_session: ActorAddress,
        operation: ActorAddress,
        replace: bool,
    },
    CancelStream {
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
    ConfigureRun {
        run_id: String,
        reply_to: ActorAddress,
    },
    Close,
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
    AttachmentDeadline,
    ReadBlob {
        path: DataPath,
        reply_to: ActorAddress,
    },
    CancelRead {
        reply_to: ActorAddress,
    },
    OpenWriteBlob {
        path: DataPath,
        length: u64,
        reply_to: ActorAddress,
    },
    OpenReadStream {
        path: DataPath,
        reply_to: ActorAddress,
        replace: bool,
    },
    OpenWriteStream {
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
    },
    ReleaseComplete(Result<(), DataPlaneError>),
}

impl NetworkMessage for HostStreamIn {
    fn type_tag() -> &'static str {
        "data-plane.host-stream.v1"
    }
}

pub fn register_data_plane_codecs(registry: &mut CodecRegistry) {
    registry.register::<HostSessionIn, _>(JsonCodec::default());
    registry.register::<ChildSessionIn, _>(JsonCodec::default());
    registry.register::<HostStreamIn, _>(JsonCodec::default());
    crate::namespace::register_namespace_codecs(registry);
    crate::blob_transfer::register_blob_transfer_codecs(registry);
    crate::source::register_blob_source_codecs(registry);
}
