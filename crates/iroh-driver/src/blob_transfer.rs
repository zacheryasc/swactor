//! Iroh implementation of one-shot fixed-length blob transfer ports.

use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use data_plane::blob_transfer::{
    BlobTransferEvent, BlobTransferId, BlobTransferOffer, BlobTransferReceiver, BlobTransferSender,
    FileTransferRequest,
};
use data_plane::edge_wire::WireEvent;
use iroh::EndpointAddr;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor_engine::{BlockingWorkSender, EngineHandle};

use crate::iroh_driver::EdgeConnector;

const FIRST_BLOB_EDGE_ID: u64 = 1 << 63;
const FILE_CHUNK_BYTES: usize = 64 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, Serialize, Deserialize)]
struct IrohBlobOffer {
    endpoint: EndpointAddr,
    edge_id: u64,
}

#[derive(Clone)]
pub struct IrohBlobTransferSender {
    connector: EdgeConnector,
    blocking: BlockingWorkSender,
    runtime: Runtime,
}

impl IrohBlobTransferSender {
    pub fn new(connector: EdgeConnector, engine: &EngineHandle, runtime: Runtime) -> Self {
        Self {
            connector,
            blocking: engine.blocking_work_sender(),
            runtime,
        }
    }
}

impl BlobTransferSender for IrohBlobTransferSender {
    fn start_file(&self, request: FileTransferRequest) -> Result<(), String> {
        let connector = self.connector.clone();
        let runtime = self.runtime.clone();
        let work = Box::new(move || {
            let result = if runtime.is_local_actor(request.offer.destination) {
                send_file_local(&runtime, &request)
            } else {
                send_file(connector, &request)
            };
            request.completion.complete(result);
        });
        self.blocking
            .submit(work)
            .map_err(|_| "blob transfer engine has stopped".to_owned())
    }
}

fn send_file(connector: EdgeConnector, request: &FileTransferRequest) -> Result<(), String> {
    let wire: IrohBlobOffer = serde_json::from_slice(&request.offer.transport)
        .map_err(|error| format!("decode Iroh blob offer: {error}"))?;
    let sender = connector.connect(wire.endpoint, wire.edge_id, CONNECT_TIMEOUT)?;
    read_file_chunks(request, |bytes| sender.send(bytes))?;
    sender.finish(CONNECT_TIMEOUT)
}

fn send_file_local(runtime: &Runtime, request: &FileTransferRequest) -> Result<(), String> {
    read_file_chunks(request, |bytes| {
        runtime
            .send_to(
                request.offer.destination,
                BlobTransferEvent::Chunk {
                    transfer_id: request.offer.transfer_id,
                    bytes,
                },
            )
            .map_err(|error| format!("send local blob chunk: {error}"))
    })?;
    runtime
        .send_to(
            request.offer.destination,
            BlobTransferEvent::Finished {
                transfer_id: request.offer.transfer_id,
            },
        )
        .map_err(|error| format!("finish local blob transfer: {error}"))
}

fn read_file_chunks(
    request: &FileTransferRequest,
    mut send: impl FnMut(Vec<u8>) -> Result<(), String>,
) -> Result<(), String> {
    let mut transferred = 0_u64;
    let mut chunk = vec![0_u8; FILE_CHUNK_BYTES];
    while transferred < request.length {
        let remaining = request.length - transferred;
        let count = usize::try_from(remaining.min(FILE_CHUNK_BYTES as u64))
            .expect("bounded blob chunk size");
        let file_offset = request
            .offset
            .checked_add(transferred)
            .ok_or_else(|| "blob source file offset overflow".to_owned())?;
        let read = request
            .file
            .read_at(&mut chunk[..count], file_offset)
            .map_err(|error| format!("read blob source at {file_offset}: {error}"))?;
        if read == 0 {
            return Err(format!(
                "blob source ended after {transferred} bytes, expected {} bytes",
                request.length
            ));
        }
        send(chunk[..read].to_vec())?;
        transferred = transferred
            .checked_add(read as u64)
            .ok_or_else(|| "blob source transfer offset overflow".to_owned())?;
    }
    Ok(())
}

#[derive(Clone)]
pub struct IrohBlobTransferReceiver {
    endpoint: EndpointAddr,
    events: Arc<Mutex<Vec<WireEvent>>>,
    destinations: Arc<Mutex<HashMap<u64, (BlobTransferId, ActorAddress)>>>,
    next_edge_id: Arc<AtomicU64>,
}

impl IrohBlobTransferReceiver {
    pub fn new(endpoint: EndpointAddr, events: Arc<Mutex<Vec<WireEvent>>>) -> Self {
        Self {
            endpoint,
            events,
            destinations: Arc::new(Mutex::new(HashMap::new())),
            next_edge_id: Arc::new(AtomicU64::new(FIRST_BLOB_EDGE_ID)),
        }
    }

    pub fn install_pump(
        self: &Arc<Self>,
        engine: &EngineHandle,
        runtime: Runtime,
        period: Duration,
    ) {
        let receiver = Arc::clone(self);
        let engine = engine.clone();
        engine.clone().spawn(async move {
            let mut interval = engine.interval(period);
            loop {
                (&mut interval).await;
                receiver.drain(&runtime);
            }
        });
    }

    pub fn drain(&self, runtime: &Runtime) {
        let mut queue = self.events.lock();
        let mut remaining = Vec::with_capacity(queue.len());
        for event in queue.drain(..) {
            match event {
                WireEvent::StreamArrived { edge_id, .. } if is_blob_edge(edge_id.0) => {}
                WireEvent::BytesRead {
                    edge_id,
                    stream_id,
                    bytes,
                } => {
                    let destination = self.destinations.lock().get(&edge_id.0).copied();
                    if let Some((transfer_id, destination)) = destination {
                        if runtime
                            .send_to(destination, BlobTransferEvent::Chunk { transfer_id, bytes })
                            .is_err()
                        {
                            // The destination actor is gone (send_to only
                            // fails for unknown addresses): the blob can no
                            // longer land anywhere and notifying anyone is
                            // impossible. Drop the mapping so later events
                            // for this edge are discarded instead of being
                            // resent to the dead actor forever.
                            self.destinations.lock().remove(&edge_id.0);
                        }
                    } else if !is_blob_edge(edge_id.0) {
                        remaining.push(WireEvent::BytesRead {
                            edge_id,
                            stream_id,
                            bytes,
                        });
                    }
                }
                WireEvent::StreamEnded { edge_id, stream_id } => {
                    if let Some((transfer_id, destination)) =
                        self.destinations.lock().remove(&edge_id.0)
                    {
                        let _ = runtime
                            .send_to(destination, BlobTransferEvent::Finished { transfer_id });
                    } else if !is_blob_edge(edge_id.0) {
                        remaining.push(WireEvent::StreamEnded { edge_id, stream_id });
                    }
                }
                WireEvent::StreamFault {
                    edge_id: Some(edge_id),
                    stream_id,
                    reason,
                } => {
                    if let Some((transfer_id, destination)) =
                        self.destinations.lock().remove(&edge_id.0)
                    {
                        let _ = runtime.send_to(
                            destination,
                            BlobTransferEvent::Failed {
                                transfer_id,
                                reason: format!("{reason:?}"),
                            },
                        );
                    } else if !is_blob_edge(edge_id.0) {
                        remaining.push(WireEvent::StreamFault {
                            edge_id: Some(edge_id),
                            stream_id,
                            reason,
                        });
                    }
                }
                event => remaining.push(event),
            }
        }
        queue.extend(remaining);
    }
}

/// Blob-transfer edges are allocated at and above [`FIRST_BLOB_EDGE_ID`],
/// disjoint from data-plane edge identifiers, so an event for such an edge
/// with no registered destination belongs to an already-terminated transfer.
fn is_blob_edge(edge_id: u64) -> bool {
    edge_id >= FIRST_BLOB_EDGE_ID
}

impl BlobTransferReceiver for IrohBlobTransferReceiver {
    fn open(
        &self,
        destination: ActorAddress,
        transfer_id: BlobTransferId,
    ) -> Result<BlobTransferOffer, String> {
        let edge_id = self.next_edge_id.fetch_add(1, Ordering::Relaxed);
        if edge_id < FIRST_BLOB_EDGE_ID {
            return Err("blob transfer edge identifiers exhausted".to_owned());
        }
        if self
            .destinations
            .lock()
            .insert(edge_id, (transfer_id, destination))
            .is_some()
        {
            return Err("blob transfer edge identifier collision".to_owned());
        }
        let transport = serde_json::to_vec(&IrohBlobOffer {
            endpoint: self.endpoint.clone(),
            edge_id,
        })
        .map_err(|error| {
            self.destinations.lock().remove(&edge_id);
            format!("encode Iroh blob offer: {error}")
        })?;
        Ok(BlobTransferOffer {
            transfer_id,
            destination,
            failure_proxy: None,
            transport,
        })
    }

    fn cancel(&self, offer: &BlobTransferOffer) {
        if let Ok(wire) = serde_json::from_slice::<IrohBlobOffer>(&offer.transport) {
            self.destinations.lock().remove(&wire.edge_id);
        }
    }
}
