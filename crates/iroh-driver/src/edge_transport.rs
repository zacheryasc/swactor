//! Driver-owned byte transport for ring-backed MVP edge protocols.
//!
//! This module deliberately owns only transport framing: one edge id preamble
//! per unidirectional stream, followed by opaque byte chunks. Object-record
//! parsing, ring ownership, and edge semantics stay in the data-plane crate,
//! which drives this transport through the `data_plane::edge_wire` port
//! (`IrohEdgePort` implements `EdgeTransport`).

use std::sync::Arc;

use data_plane::edge_wire::{EdgeWriter, WireEvent, WireFault};
use data_plane::ids::{EdgeId, StreamId};
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr};
use parking_lot::Mutex;
use swactor_engine::{EngineHandle, TaskSpawner};
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc as tokio_mpsc;

use crate::iroh_driver::run_until_closed;

pub const EDGE_ALPN: &[u8] = b"mvp/pipeline-edge/0";

/// One-shot completion receiver observed by [`EdgeSendHandle::finish`].
type CompletionReceiver = std::sync::mpsc::Receiver<Result<(), String>>;

/// Cloneable handle for pushing opaque record bytes onto one edge's send
/// pump. Implements the data-plane [`EdgeWriter`] port.
#[derive(Clone)]
pub struct EdgeSendHandle {
    tx: tokio_mpsc::UnboundedSender<Vec<u8>>,
    completion: Arc<Mutex<Option<CompletionReceiver>>>,
    owner: Arc<EdgeSendOwner>,
}

struct EdgeSendOwner(tokio::sync::watch::Sender<bool>);

impl Drop for EdgeSendOwner {
    fn drop(&mut self) {
        self.0.send_replace(true);
    }
}

/// One edge pump request. The sender owns no executor or task capability.
pub(crate) struct EdgeSendRequest {
    peer: EndpointAddr,
    edge_id: u64,
    rx: tokio_mpsc::UnboundedReceiver<Vec<u8>>,
    ready_tx: std::sync::mpsc::Sender<Result<(), String>>,
    completion_tx: std::sync::mpsc::Sender<Result<(), String>>,
    cancellation: tokio::sync::watch::Receiver<bool>,
}

impl EdgeSendRequest {
    pub(crate) fn new(
        peer: EndpointAddr,
        edge_id: u64,
    ) -> (EdgeSendHandle, Self, CompletionReceiver) {
        let (tx, rx) = tokio_mpsc::unbounded_channel();
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        let (completion_tx, completion_rx) = std::sync::mpsc::channel();
        let (cancel, cancellation) = tokio::sync::watch::channel(false);
        let handle = EdgeSendHandle {
            tx,
            completion: Arc::new(Mutex::new(Some(completion_rx))),
            owner: Arc::new(EdgeSendOwner(cancel)),
        };
        let request = Self {
            peer,
            edge_id,
            rx,
            ready_tx,
            completion_tx,
            cancellation,
        };
        (handle, request, ready_rx)
    }
}

/// A cancelled write must reset, not implicitly finish buffered partial
/// records when Quinn's send stream drops.
pub(crate) struct ResetOnDrop(pub(crate) iroh::endpoint::SendStream);

impl std::ops::Deref for ResetOnDrop {
    type Target = iroh::endpoint::SendStream;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ResetOnDrop {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for ResetOnDrop {
    fn drop(&mut self) {
        let _ = self.0.reset(iroh::endpoint::VarInt::from_u32(0));
    }
}

impl EdgeSendHandle {
    pub fn send(&self, bytes: Vec<u8>) -> Result<(), String> {
        self.tx
            .send(bytes)
            .map_err(|_| "edge sender task stopped".to_owned())
    }
    pub fn finish(self, timeout: std::time::Duration) -> Result<(), String> {
        let Self {
            tx,
            completion,
            owner,
        } = self;
        drop(tx);
        let result = completion
            .lock()
            .take()
            .ok_or_else(|| "edge sender completion was already observed".to_owned())?
            .recv_timeout(timeout)
            .map_err(|error| format!("edge sender completion: {error}"));
        if result.is_err() {
            owner.0.send_replace(true);
        }
        result?
    }
}

impl EdgeWriter for EdgeSendHandle {
    fn send(&self, bytes: Vec<u8>) -> Result<(), String> {
        self.send(bytes)
    }
}

pub(crate) fn spawn_edge_send_pump(
    tasks: &TaskSpawner,
    engine: EngineHandle,
    endpoint: Endpoint,
    request: EdgeSendRequest,
    events: Arc<Mutex<Vec<WireEvent>>>,
    activity: tokio::sync::watch::Sender<()>,
    changes: tokio::sync::watch::Receiver<u64>,
) {
    let EdgeSendRequest {
        peer,
        edge_id,
        mut rx,
        ready_tx,
        completion_tx,
        mut cancellation,
    } = request;
    let engine_handle = engine;
    tasks.spawn(async move {
        let operation = async {
            macro_rules! open_edge_stream {
                () => {{
                    let conn = endpoint
                        .connect(peer.clone(), EDGE_ALPN)
                        .await
                        .map_err(|e| format!("connect edge {edge_id}: {e}"))?;
                    let mut send = ResetOnDrop(
                        conn.open_uni()
                            .await
                            .map_err(|e| format!("open edge stream {edge_id}: {e}"))?,
                    );
                    send.write_all(&encode_edge_preamble(edge_id))
                        .await
                        .map_err(|e| format!("write edge preamble {edge_id}: {e}"))?;
                    send.flush()
                        .await
                        .map_err(|e| format!("flush edge preamble {edge_id}: {e}"))?;
                    send
                }};
            }

            let mut send = open_edge_stream!();
            let _ = ready_tx.send(Ok(()));
            while let Some(record) = rx.recv().await {
                let mut attempts = 0_u8;
                loop {
                    attempts = attempts.saturating_add(1);
                    let write_result = engine_handle
                        .timeout(std::time::Duration::from_secs(30), async {
                            send.write_all(&record)
                                .await
                                .map_err(|e| format!("write edge record {edge_id}: {e}"))?;
                            send.flush()
                                .await
                                .map_err(|e| format!("flush edge record {edge_id}: {e}"))
                        })
                        .await
                        .map_err(|_| format!("write edge record {edge_id}: timed out"))?;

                    match write_result {
                        Ok(()) => break,
                        Err(_error) if attempts < 3 => {
                            // A dropped quinn SendStream is NOT reset: bytes
                            // already buffered on the failed attempt can
                            // still arrive after we move on, duplicating or
                            // corrupting the record. Reset explicitly so the
                            // receiver observes a read fault, discards the
                            // partial edge state, and only then reads the
                            // resend on the fresh stream.
                            let _ = send.reset(iroh::endpoint::VarInt::from_u32(0));
                            send = open_edge_stream!();
                            continue;
                        }
                        Err(error) => {
                            let _ = send.reset(iroh::endpoint::VarInt::from_u32(0));
                            return Err(error);
                        }
                    }
                }
            }
            send.finish()
                .map_err(|e| format!("finish edge stream {edge_id}: {e}"))?;
            match engine_handle
                .timeout(std::time::Duration::from_secs(30), send.stopped())
                .await
                .map_err(|_| format!("finish edge stream {edge_id}: timed out"))?
                .map_err(|e| format!("finish edge stream {edge_id}: {e}"))?
            {
                Some(code) => Err(format!("peer stopped edge stream {edge_id}: {code}")),
                None => Ok(()),
            }
        };
        let result: Result<(), String> = tokio::select! {
            biased;
            _ = cancellation.changed() => Err(format!("edge {edge_id} owner cancelled")),
            result = run_until_closed(changes, operation) => {
                result.unwrap_or_else(|| Err(format!("edge {edge_id} driver closed")))
            },
        };
        if let Err(error) = &result {
            let _ = ready_tx.send(Err(error.clone()));
            events.lock().push(WireEvent::StreamFault {
                edge_id: Some(EdgeId(edge_id)),
                stream_id: None,
                reason: WireFault::WriteError,
            });
            activity.send_replace(());
        }
        let _ = completion_tx.send(result);
    });
}

pub(crate) fn spawn_edge_recv_pump(
    tasks: TaskSpawner,
    conn: Connection,
    events: Arc<Mutex<Vec<WireEvent>>>,
    stream_group: u64,
    activity: tokio::sync::watch::Sender<()>,
    changes: tokio::sync::watch::Receiver<u64>,
) {
    tasks.spawn(async move {
        run_until_closed(changes, async {
            let mut next_uni_stream_id = stream_group << 32;
            while let Ok(mut recv) = conn.accept_uni().await {
                next_uni_stream_id = next_uni_stream_id.saturating_add(1);
                let current_stream_id = StreamId(next_uni_stream_id);
                let mut preamble = [0u8; 8];
                if recv.read_exact(&mut preamble).await.is_err() {
                    events.lock().push(WireEvent::StreamFault {
                        edge_id: None,
                        stream_id: Some(current_stream_id),
                        reason: WireFault::ProtocolError,
                    });
                    activity.send_replace(());
                    continue;
                }
                let edge_id = EdgeId(u64::from_le_bytes(preamble));
                events.lock().push(WireEvent::StreamArrived {
                    edge_id,
                    stream_id: current_stream_id,
                });
                activity.send_replace(());
                let mut chunk = vec![0u8; 4096];
                loop {
                    match recv.read(&mut chunk).await {
                        Ok(Some(0)) | Ok(None) => {
                            events.lock().push(WireEvent::StreamEnded {
                                edge_id,
                                stream_id: current_stream_id,
                            });
                            activity.send_replace(());
                            break;
                        }
                        Ok(Some(n)) => {
                            events.lock().push(WireEvent::BytesRead {
                                edge_id,
                                stream_id: current_stream_id,
                                bytes: chunk[..n].to_vec(),
                            });
                            activity.send_replace(());
                        }
                        Err(_) => {
                            events.lock().push(WireEvent::StreamFault {
                                edge_id: Some(edge_id),
                                stream_id: Some(current_stream_id),
                                reason: WireFault::ReadError,
                            });
                            activity.send_replace(());
                            break;
                        }
                    }
                }
            }
        })
        .await;
    });
}

fn encode_edge_preamble(edge_id: u64) -> [u8; 8] {
    edge_id.to_le_bytes()
}
