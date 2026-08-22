//! Persistent fixed-length blob sources.

use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::Runtime;
use swactor_transport::{CodecRegistry, JsonCodec, NetworkMessage};

use crate::blob_transfer::{
    BlobTransferCompletion, BlobTransferEvent, BlobTransferId, BlobTransferOffer,
    BlobTransferSender, FileTransferRequest,
};
use crate::namespace::{NamespaceClientIn, NamespaceError};
use crate::namespace_store::SourceRecovery;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum BlobSourceIn {
    BeginTransfer {
        offer: BlobTransferOffer,
    },
    TransferCompleted {
        transfer_id: BlobTransferId,
        destination: ActorAddress,
        failure_proxy: Option<ActorAddress>,
        result: Result<(), String>,
    },
    Retire,
}

impl NetworkMessage for BlobSourceIn {
    fn type_tag() -> &'static str {
        "data-plane.blob-source.in.v1"
    }
}

struct ActorTransferCompletion {
    runtime: Runtime,
    source: ActorAddress,
    transfer_id: BlobTransferId,
    destination: ActorAddress,
    failure_proxy: Option<ActorAddress>,
}

impl BlobTransferCompletion for ActorTransferCompletion {
    fn complete(self: Box<Self>, result: Result<(), String>) {
        let _ = self.runtime.send_to(
            self.source,
            BlobSourceIn::TransferCompleted {
                transfer_id: self.transfer_id,
                destination: self.destination,
                failure_proxy: self.failure_proxy,
                result,
            },
        );
    }
}

pub trait BlobSourcePublisher: Send + Sync + 'static {
    fn publish_source(&self, source: ActorAddress) -> Result<(), String>;
}

pub trait BlobSourceRetirement: Send + Sync + 'static {
    fn retired(&self);
}

pub struct FileBlobSourceActor {
    runtime: Runtime,
    sender: Arc<dyn BlobTransferSender>,
    recovery_path: Option<PathBuf>,
    label: String,
    file: File,
    offset: u64,
    length: u64,
    active: HashSet<BlobTransferId>,
    retiring: bool,
    retirement: Option<Arc<dyn BlobSourceRetirement>>,
}

impl FileBlobSourceActor {
    pub fn open(
        runtime: Runtime,
        sender: Arc<dyn BlobTransferSender>,
        path: impl AsRef<Path>,
    ) -> Result<Self, NamespaceError> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).open(&path).map_err(|error| {
            NamespaceError::SourceRecovery(format!("open {}: {error}", path.display()))
        })?;
        let length = file
            .metadata()
            .map_err(|error| {
                NamespaceError::SourceRecovery(format!("stat {}: {error}", path.display()))
            })?
            .len();
        Ok(Self {
            runtime,
            sender,
            recovery_path: Some(path.clone()),
            label: path.display().to_string(),
            file,
            offset: 0,
            length,
            active: HashSet::new(),
            retiring: false,
            retirement: None,
        })
    }

    pub fn recover(
        runtime: Runtime,
        sender: Arc<dyn BlobTransferSender>,
        path: impl AsRef<Path>,
        expected_length: u64,
    ) -> Result<Self, NamespaceError> {
        let source = Self::open(runtime, sender, path)?;
        if source.length != expected_length {
            return Err(NamespaceError::SourceRecovery(format!(
                "recovered file {} has length {}, expected {}",
                source.label, source.length, expected_length
            )));
        }
        Ok(source)
    }

    pub fn from_file_region(
        runtime: Runtime,
        sender: Arc<dyn BlobTransferSender>,
        file: File,
        offset: u64,
        length: u64,
        retirement: Option<Arc<dyn BlobSourceRetirement>>,
    ) -> Result<Self, NamespaceError> {
        let file_length = file
            .metadata()
            .map_err(|error| NamespaceError::SourceRecovery(format!("stat arena source: {error}")))?
            .len();
        let end = offset.checked_add(length).ok_or_else(|| {
            NamespaceError::SourceRecovery("arena source range overflow".to_owned())
        })?;
        if end > file_length {
            return Err(NamespaceError::SourceRecovery(format!(
                "arena source range ends at {end}, backing length is {file_length}"
            )));
        }
        Ok(Self {
            runtime,
            sender,
            recovery_path: None,
            label: format!("arena region {offset}..{end}"),
            file,
            offset,
            length,
            active: HashSet::new(),
            retiring: false,
            retirement,
        })
    }

    pub fn length(&self) -> u64 {
        self.length
    }

    pub fn recovery(&self) -> SourceRecovery {
        SourceRecovery::File {
            path: self
                .recovery_path
                .clone()
                .expect("disk source has a recovery path"),
        }
    }

    fn send_failure(
        &self,
        ctx: &Ctx<'_>,
        destination: ActorAddress,
        failure_proxy: Option<ActorAddress>,
        transfer_id: BlobTransferId,
        reason: String,
    ) {
        if let Some(proxy) = failure_proxy {
            let _ = ctx.send(
                proxy,
                NamespaceClientIn::TransferFailed {
                    destination,
                    transfer_id,
                    reason,
                },
            );
        } else {
            let _ = ctx.send(
                destination,
                BlobTransferEvent::Failed {
                    transfer_id,
                    reason,
                },
            );
        }
    }

    fn fail_destination(&self, ctx: &Ctx<'_>, offer: &BlobTransferOffer, reason: String) {
        self.send_failure(
            ctx,
            offer.destination,
            offer.failure_proxy,
            offer.transfer_id,
            reason,
        );
    }

    fn maybe_stop_retired(&mut self, ctx: &Ctx<'_>) {
        if self.retiring && self.active.is_empty() {
            if let Some(retirement) = self.retirement.take() {
                retirement.retired();
            }
            ctx.stop_self();
        }
    }
}

impl ActorInterface for FileBlobSourceActor {
    type Incoming = BlobSourceIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, message: BlobSourceIn) {
        match message {
            BlobSourceIn::BeginTransfer { offer } => {
                if self.retiring {
                    self.fail_destination(ctx, &offer, "blob source is retired".to_owned());
                    return;
                }
                if !self.active.insert(offer.transfer_id) {
                    self.fail_destination(
                        ctx,
                        &offer,
                        "duplicate blob transfer identifier".to_owned(),
                    );
                    return;
                }
                let file = match self.file.try_clone() {
                    Ok(file) => file,
                    Err(error) => {
                        self.active.remove(&offer.transfer_id);
                        self.fail_destination(
                            ctx,
                            &offer,
                            format!("clone source file {}: {error}", self.label),
                        );
                        return;
                    }
                };
                let completion: Box<dyn BlobTransferCompletion> =
                    Box::new(ActorTransferCompletion {
                        runtime: self.runtime.clone(),
                        source: ctx.self_addr(),
                        transfer_id: offer.transfer_id,
                        destination: offer.destination,
                        failure_proxy: offer.failure_proxy,
                    });
                let request = FileTransferRequest {
                    offer: offer.clone(),
                    file,
                    offset: self.offset,
                    length: self.length,
                    completion,
                };
                if let Err(error) = self.sender.start_file(request) {
                    self.active.remove(&offer.transfer_id);
                    self.fail_destination(ctx, &offer, error);
                    self.maybe_stop_retired(ctx);
                }
            }
            BlobSourceIn::TransferCompleted {
                transfer_id,
                destination,
                failure_proxy,
                result,
            } => {
                if self.active.remove(&transfer_id) {
                    if let Err(reason) = result {
                        self.send_failure(ctx, destination, failure_proxy, transfer_id, reason);
                    }
                    self.maybe_stop_retired(ctx);
                }
            }
            BlobSourceIn::Retire => {
                self.retiring = true;
                self.maybe_stop_retired(ctx);
            }
        }
    }
}

pub fn register_blob_source_codecs(registry: &mut CodecRegistry) {
    registry.register::<BlobSourceIn, _>(JsonCodec::default());
}
