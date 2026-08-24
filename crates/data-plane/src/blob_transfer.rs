//! Transport-neutral contracts for one-shot fixed-length blob transfers.

use std::fmt;
use std::fs::File;

use crate::blob::{BlobLease, BlobMetadata};
use crate::protocol::DataPlaneError;
use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor_transport::{CodecRegistry, JsonCodec, NetworkMessage};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BlobTransferId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobTransferOffer {
    pub transfer_id: BlobTransferId,
    pub destination: ActorAddress,
    pub failure_proxy: Option<ActorAddress>,
    pub transport: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BlobTransferEvent {
    Chunk {
        transfer_id: BlobTransferId,
        bytes: Vec<u8>,
    },
    Finished {
        transfer_id: BlobTransferId,
    },
    Failed {
        transfer_id: BlobTransferId,
        reason: String,
    },
    Allocated(Result<(BlobLease, BlobMetadata), DataPlaneError>),
    AllocatorFailed(DataPlaneError),
    Sealed(Result<(), DataPlaneError>),
    Released(Result<(), DataPlaneError>),
    Cancel,
}

impl NetworkMessage for BlobTransferEvent {
    fn type_tag() -> &'static str {
        "data-plane.blob-transfer.event.v1"
    }
}

pub trait BlobTransferCompletion: Send + 'static {
    fn complete(self: Box<Self>, result: Result<(), String>);
}

pub struct FileTransferRequest {
    pub offer: BlobTransferOffer,
    pub file: File,
    pub offset: u64,
    pub length: u64,
    pub completion: Box<dyn BlobTransferCompletion>,
}

pub trait BlobTransferSender: Send + Sync + 'static {
    fn start_file(&self, request: FileTransferRequest) -> Result<(), String>;
}

pub trait BlobTransferReceiver: Send + Sync + 'static {
    fn open(
        &self,
        destination: ActorAddress,
        transfer_id: BlobTransferId,
    ) -> Result<BlobTransferOffer, String>;

    fn cancel(&self, offer: &BlobTransferOffer);
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlobTransferFailure {
    Start(String),
    Source(String),
    Transport(String),
    Length { expected: u64, found: u64 },
}

impl fmt::Display for BlobTransferFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start(reason) => write!(f, "blob transfer did not start: {reason}"),
            Self::Source(reason) => write!(f, "blob source failed: {reason}"),
            Self::Transport(reason) => write!(f, "blob transport failed: {reason}"),
            Self::Length { expected, found } => {
                write!(
                    f,
                    "blob transfer length mismatch: expected {expected}, found {found}"
                )
            }
        }
    }
}

impl std::error::Error for BlobTransferFailure {}

pub fn register_blob_transfer_codecs(registry: &mut CodecRegistry) {
    registry
        .register::<BlobTransferEvent, _>(JsonCodec::default())
        .expect("unique codec registration");
}
