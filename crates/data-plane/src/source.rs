//! Persistent fixed-length blob sources.

use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use protocol_telemetry::{
    ActorType, Outcome, Phase, Protocol, Reason, TransitionEmitter, TransitionRecord,
};
use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::Runtime;
use swactor_transport::{CodecRegistry, JsonCodec, NetworkMessage};

use crate::blob_transfer::{
    BlobTransferCompletion, BlobTransferEvent, BlobTransferId, BlobTransferOffer,
    BlobTransferSender, FileTransferRequest,
};
use crate::namespace::{DataDirectoryIn, NamespaceClientIn, NamespaceError};
use crate::namespace_store::SourceRecovery;
const COMPLETED_TRANSFER_HISTORY: usize = 1_024;

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
    Retire {
        reply_to: Option<ActorAddress>,
    },
    InspectResources {
        request_id: String,
        reply_to: ActorAddress,
    },
}

impl NetworkMessage for BlobSourceIn {
    fn type_tag() -> &'static str {
        "data-plane.blob-source.in.v1"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlobSourceResources {
    pub request_id: String,
    pub source: ActorAddress,
    pub binding: Option<ActorAddress>,
    pub active_transfers: usize,
    pub retiring: bool,
}

impl NetworkMessage for BlobSourceResources {
    fn type_tag() -> &'static str {
        "data-plane.blob-source.resources.v1"
    }
}

struct ActorTransferCompletion {
    runtime: Runtime,
    source: ActorAddress,
    transfer_id: Option<BlobTransferId>,
    destination: ActorAddress,
    failure_proxy: Option<ActorAddress>,
    _retirement: Option<Arc<RetainedSource>>,
}

impl BlobTransferCompletion for ActorTransferCompletion {
    fn complete(mut self: Box<Self>, result: Result<(), String>) {
        let _ = self.runtime.send_to(
            self.source,
            BlobSourceIn::TransferCompleted {
                transfer_id: self.transfer_id.take().expect("pending completion"),
                destination: self.destination,
                failure_proxy: self.failure_proxy,
                result,
            },
        );
    }
}

impl Drop for ActorTransferCompletion {
    fn drop(&mut self) {
        if let Some(transfer_id) = self.transfer_id.take() {
            let _ = self.runtime.send_to(
                self.source,
                BlobSourceIn::TransferCompleted {
                    transfer_id,
                    destination: self.destination,
                    failure_proxy: self.failure_proxy,
                    result: Err("blob transfer owner dropped before completion".to_owned()),
                },
            );
        }
    }
}

pub trait BlobSourcePublisher: Send + Sync + 'static {
    fn publish_source(&self, source: ActorAddress) -> Result<[u8; 32], String>;
}

pub trait BlobSourceRetirement: Send + Sync + 'static {
    fn retired(&self);
    fn binding(&self) -> ActorAddress;
}

/// The backing region remains leased through every adapter-owned transfer,
/// even if the source actor is stopped before the adapter completes.
struct RetainedSource(Arc<dyn BlobSourceRetirement>);

impl Drop for RetainedSource {
    fn drop(&mut self) {
        self.0.retired();
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SourcePhase {
    Available,
    Retiring,
    Closed,
}

pub struct FileBlobSourceActor {
    runtime: Runtime,
    sender: Arc<dyn BlobTransferSender>,
    recovery_path: Option<PathBuf>,
    label: String,
    file: File,
    offset: u64,
    length: u64,
    active: HashMap<BlobTransferId, (ActorAddress, Option<ActorAddress>)>,
    completed: HashSet<BlobTransferId>,
    completed_order: VecDeque<BlobTransferId>,
    phase: SourcePhase,
    retirement: Option<Arc<RetainedSource>>,
    transitions: Option<TransitionEmitter>,
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
            active: HashMap::new(),
            completed: HashSet::new(),
            completed_order: VecDeque::new(),
            phase: SourcePhase::Available,
            transitions: None,
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
            active: HashMap::new(),
            completed: HashSet::new(),
            completed_order: VecDeque::new(),
            phase: SourcePhase::Available,
            transitions: None,
            retirement: retirement.map(|retirement| Arc::new(RetainedSource(retirement))),
        })
    }

    pub fn with_transitions(mut self, transitions: Option<TransitionEmitter>) -> Self {
        self.transitions = transitions;
        self
    }

    fn enter_phase(&mut self, ctx: &Ctx<'_>, next: SourcePhase, reason: Reason) {
        let previous = std::mem::replace(&mut self.phase, next);
        if previous == next {
            return;
        }
        let phase = |phase| match phase {
            SourcePhase::Available => Phase::Ready,
            SourcePhase::Retiring => Phase::Releasing,
            SourcePhase::Closed => Phase::Closed,
        };
        if let Some(emitter) = &self.transitions {
            emitter.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::FileBlobSourceActor,
                protocol: Protocol::BlobSource,
                operation: None,
                generation: None,
                incarnation: None,
                previous: phase(previous),
                next: phase(next),
                reason,
                outcome: (next == SourcePhase::Closed).then_some(if reason == Reason::Shutdown {
                    Outcome::Cancelled
                } else {
                    Outcome::Success
                }),
            });
        }
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

    fn remember_completed(&mut self, transfer_id: BlobTransferId) {
        if !self.completed.insert(transfer_id) {
            return;
        }
        self.completed_order.push_back(transfer_id);
        if self.completed_order.len() > COMPLETED_TRANSFER_HISTORY
            && let Some(expired) = self.completed_order.pop_front()
        {
            self.completed.remove(&expired);
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
        if self.phase == SourcePhase::Retiring && self.active.is_empty() {
            self.enter_phase(ctx, SourcePhase::Closed, Reason::Completed);
            self.retirement.take();
            ctx.stop_self();
        }
    }

    fn emit_transfer(
        &self,
        ctx: &Ctx<'_>,
        id: BlobTransferId,
        previous: Phase,
        next: Phase,
        reason: Reason,
        outcome: Option<Outcome>,
    ) {
        if let Some(emitter) = &self.transitions {
            emitter.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::FileBlobSourceActor,
                protocol: Protocol::BlobSource,
                operation: Some(u128::from(id.0).to_be_bytes()),
                generation: None,
                incarnation: None,
                previous,
                next,
                reason,
                outcome,
            });
        }
    }

    fn begin_transfer(&mut self, ctx: &Ctx<'_>, offer: BlobTransferOffer) {
        if self.phase != SourcePhase::Available {
            self.fail_destination(ctx, &offer, "blob source is retired".to_owned());
            return;
        }
        if self.completed.contains(&offer.transfer_id)
            || self.active.contains_key(&offer.transfer_id)
        {
            return;
        }
        let file = match self.file.try_clone() {
            Ok(file) => file,
            Err(error) => {
                self.fail_destination(
                    ctx,
                    &offer,
                    format!("clone source file {}: {error}", self.label),
                );
                return;
            }
        };
        let transfer_id = offer.transfer_id;
        let destination = offer.destination;
        let failure_proxy = offer.failure_proxy;
        self.active
            .insert(transfer_id, (destination, failure_proxy));
        self.emit_transfer(
            ctx,
            transfer_id,
            Phase::Created,
            Phase::Transferring,
            Reason::Started,
            None,
        );
        let completion = Box::new(ActorTransferCompletion {
            runtime: self.runtime.clone(),
            source: ctx.self_addr(),
            transfer_id: Some(transfer_id),
            destination,
            failure_proxy,
            _retirement: self.retirement.clone(),
        });
        let request = FileTransferRequest {
            offer,
            file,
            offset: self.offset,
            length: self.length,
            completion,
        };
        if let Err(error) = self.sender.start_file(request) {
            self.transfer_completed(ctx, transfer_id, destination, failure_proxy, Err(error));
        }
    }

    fn transfer_completed(
        &mut self,
        ctx: &Ctx<'_>,
        transfer_id: BlobTransferId,
        destination: ActorAddress,
        failure_proxy: Option<ActorAddress>,
        result: Result<(), String>,
    ) {
        if self.active.get(&transfer_id) != Some(&(destination, failure_proxy)) {
            return;
        }
        self.active.remove(&transfer_id);
        self.emit_transfer(
            ctx,
            transfer_id,
            Phase::Transferring,
            Phase::Completed,
            Reason::Completed,
            Some(if result.is_ok() {
                Outcome::Success
            } else {
                Outcome::Failed
            }),
        );
        match result {
            Ok(()) => self.remember_completed(transfer_id),
            Err(reason) => self.send_failure(ctx, destination, failure_proxy, transfer_id, reason),
        }
        self.maybe_stop_retired(ctx);
    }

    fn retire(&mut self, ctx: &Ctx<'_>, reply_to: Option<ActorAddress>) {
        self.enter_phase(ctx, SourcePhase::Retiring, Reason::Retired);
        if let Some(reply_to) = reply_to {
            let _ = ctx.send(
                reply_to,
                DataDirectoryIn::SourceRetired {
                    source: ctx.self_addr(),
                },
            );
        }
        self.maybe_stop_retired(ctx);
    }
}

impl ActorInterface for FileBlobSourceActor {
    type Incoming = BlobSourceIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, message: BlobSourceIn) {
        match message {
            BlobSourceIn::InspectResources {
                request_id,
                reply_to,
            } => {
                let _ = ctx.send(
                    reply_to,
                    BlobSourceResources {
                        request_id,
                        source: ctx.self_addr(),
                        binding: self
                            .retirement
                            .as_ref()
                            .map(|retirement| retirement.0.binding()),
                        active_transfers: self.active.len(),
                        retiring: self.phase != SourcePhase::Available,
                    },
                );
            }
            BlobSourceIn::BeginTransfer { offer } => self.begin_transfer(ctx, offer),
            BlobSourceIn::TransferCompleted {
                transfer_id,
                destination,
                failure_proxy,
                result,
            } => self.transfer_completed(ctx, transfer_id, destination, failure_proxy, result),
            BlobSourceIn::Retire { reply_to } => self.retire(ctx, reply_to),
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        for (transfer_id, (destination, failure_proxy)) in std::mem::take(&mut self.active) {
            self.emit_transfer(
                ctx,
                transfer_id,
                Phase::Transferring,
                Phase::Closed,
                Reason::Shutdown,
                Some(Outcome::Cancelled),
            );
            self.send_failure(
                ctx,
                destination,
                failure_proxy,
                transfer_id,
                "blob source stopped during transfer".to_owned(),
            );
        }
        self.enter_phase(ctx, SourcePhase::Closed, Reason::Shutdown);
        self.retirement.take();
    }
}

pub fn register_blob_source_codecs(registry: &mut CodecRegistry) {
    registry
        .register::<BlobSourceIn, _>(JsonCodec::default())
        .expect("unique codec registration");
    registry
        .register::<BlobSourceResources, _>(JsonCodec::default())
        .expect("unique codec registration");
}
