//! Driver-owned byte transport for ring-backed MVP edge protocols.
//!
//! This module deliberately owns only transport framing: one edge id preamble
//! per unidirectional stream, followed by opaque byte chunks. Object-record
//! parsing, ring ownership, and edge semantics stay in the data-plane crate,
//! which drives this transport through the `data_plane::edge_wire` port
//! (`IrohDriver` implements `EdgeTransport`).

use std::sync::Arc;

use data_plane::edge_wire::{EdgeWriter, WireEvent, WireFault};
use data_plane::ids::{EdgeId, StreamId};
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr};
use parking_lot::Mutex;
use swactor_engine::EngineHandle;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc as tokio_mpsc;

pub const EDGE_ALPN: &[u8] = b"mvp/pipeline-edge/0";

/// Cloneable handle for pushing opaque record bytes onto one edge's send
/// pump. Implements the data-plane [`EdgeWriter`] port.
#[derive(Clone)]
pub struct EdgeSendHandle {
    tx: tokio_mpsc::UnboundedSender<Vec<u8>>,
}

impl EdgeSendHandle {
    pub fn send(&self, bytes: Vec<u8>) -> Result<(), String> {
        self.tx
            .send(bytes)
            .map_err(|_| "edge sender task stopped".to_owned())
    }
}

impl EdgeWriter for EdgeSendHandle {
    fn send(&self, bytes: Vec<u8>) -> Result<(), String> {
        self.send(bytes)
    }
}

pub(crate) fn spawn_edge_send_pump(
    engine: EngineHandle,
    endpoint: Endpoint,
    peer: EndpointAddr,
    edge_id: u64,
    ready_timeout: Option<std::time::Duration>,
) -> Result<EdgeSendHandle, String> {
    let (tx, mut rx) = tokio_mpsc::unbounded_channel::<Vec<u8>>();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();
    let engine_handle = engine.clone();
    engine.spawn(async move {
        let result: Result<(), String> = async {
            macro_rules! open_edge_stream {
                () => {{
                    let conn = endpoint
                        .connect(peer.clone(), EDGE_ALPN)
                        .await
                        .map_err(|e| format!("connect edge {edge_id}: {e}"))?;
                    let mut send = conn
                        .open_uni()
                        .await
                        .map_err(|e| format!("open edge stream {edge_id}: {e}"))?;
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
                            send = open_edge_stream!();
                            continue;
                        }
                        Err(error) => return Err(error),
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
                Some(code) => return Err(format!("peer stopped edge stream {edge_id}: {code}")),
                None => Ok(()),
            }
        }
        .await;
        if let Err(error) = result {
            let _ = ready_tx.send(Err(error));
        }
    });
    match ready_timeout {
        Some(timeout) => ready_rx
            .recv_timeout(timeout)
            .map_err(|error| format!("edge {edge_id} sender startup: {error}"))??,
        None => ready_rx
            .recv()
            .map_err(|error| format!("edge {edge_id} sender startup channel closed: {error}"))??,
    }
    Ok(EdgeSendHandle { tx })
}

pub(crate) fn spawn_edge_recv_pump(
    engine: EngineHandle,
    conn: Connection,
    events: Arc<Mutex<Vec<WireEvent>>>,
    stream_group: u64,
) {
    engine.spawn(async move {
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
                continue;
            }
            let edge_id = EdgeId(u64::from_le_bytes(preamble));
            events.lock().push(WireEvent::StreamArrived {
                edge_id,
                stream_id: current_stream_id,
            });
            let mut chunk = vec![0u8; 4096];
            loop {
                match recv.read(&mut chunk).await {
                    Ok(Some(0)) | Ok(None) => {
                        events.lock().push(WireEvent::StreamEnded {
                            edge_id,
                            stream_id: current_stream_id,
                        });
                        break;
                    }
                    Ok(Some(n)) => {
                        events.lock().push(WireEvent::BytesRead {
                            edge_id,
                            stream_id: current_stream_id,
                            bytes: chunk[..n].to_vec(),
                        });
                    }
                    Err(_) => {
                        events.lock().push(WireEvent::StreamFault {
                            edge_id: Some(edge_id),
                            stream_id: Some(current_stream_id),
                            reason: WireFault::ReadError,
                        });
                        break;
                    }
                }
            }
        }
    });
}

fn encode_edge_preamble(edge_id: u64) -> [u8; 8] {
    edge_id.to_le_bytes()
}
