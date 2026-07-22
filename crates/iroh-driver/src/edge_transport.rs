//! Driver-owned byte transport for ring-backed MVP edge protocols.
//!
//! This module deliberately owns only transport framing: one edge id preamble per
//! unidirectional stream, followed by opaque byte chunks. Object-record parsing,
//! ring ownership, and stage semantics stay in the MVP/dataplane crates.

use std::sync::Arc;

use distribution::types::NodeId;
use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr};
use parking_lot::Mutex;
use tokio::io::AsyncWriteExt;
use tokio::runtime::Handle;
use tokio::sync::mpsc as tokio_mpsc;

pub const EDGE_ALPN: &[u8] = b"mvp/pipeline-edge/0";

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EdgeTransportEvent {
    StreamArrived {
        peer: NodeId,
        edge_id: u64,
        stream_id: u64,
    },
    BytesRead {
        peer: NodeId,
        edge_id: u64,
        stream_id: u64,
        bytes: Vec<u8>,
    },
    StreamEnded {
        peer: NodeId,
        edge_id: u64,
        stream_id: u64,
    },
    StreamFault {
        peer: NodeId,
        edge_id: Option<u64>,
        stream_id: Option<u64>,
        reason: EdgeTransportFault,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeTransportFault {
    ReadError,
    WriteError,
    ProtocolError,
}

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

pub(crate) fn spawn_edge_send_pump(
    handle: Handle,
    endpoint: Endpoint,
    peer: EndpointAddr,
    edge_id: u64,
) -> Result<EdgeSendHandle, String> {
    let (tx, mut rx) = tokio_mpsc::unbounded_channel::<Vec<u8>>();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel::<Result<(), String>>();
    handle.spawn(async move {
        let result: Result<(), String> = async {
            let conn = endpoint
                .connect(peer, EDGE_ALPN)
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
            let _ = ready_tx.send(Ok(()));
            while let Some(record) = rx.recv().await {
                send.write_all(&record)
                    .await
                    .map_err(|e| format!("write edge record {edge_id}: {e}"))?;
                send.flush()
                    .await
                    .map_err(|e| format!("flush edge record {edge_id}: {e}"))?;
            }
            send.finish()
                .map_err(|e| format!("finish edge stream {edge_id}: {e}"))?;
            Ok(())
        }
        .await;
        if let Err(error) = result {
            let _ = ready_tx.send(Err(error));
        }
    });
    ready_rx
        .recv()
        .map_err(|e| format!("edge {edge_id} sender startup channel closed: {e}"))??;
    Ok(EdgeSendHandle { tx })
}

pub(crate) fn spawn_edge_recv_pump(
    handle: Handle,
    conn: Connection,
    peer: NodeId,
    events: Arc<Mutex<Vec<EdgeTransportEvent>>>,
    stream_group: u64,
) {
    handle.spawn(async move {
        let mut next_uni_stream_id = stream_group << 32;
        while let Ok(mut recv) = conn.accept_uni().await {
            next_uni_stream_id = next_uni_stream_id.saturating_add(1);
            let current_stream_id = next_uni_stream_id;
            let mut preamble = [0u8; 8];
            if recv.read_exact(&mut preamble).await.is_err() {
                events.lock().push(EdgeTransportEvent::StreamFault {
                    peer,
                    edge_id: None,
                    stream_id: Some(current_stream_id),
                    reason: EdgeTransportFault::ProtocolError,
                });
                continue;
            }
            let edge_id = u64::from_le_bytes(preamble);
            events.lock().push(EdgeTransportEvent::StreamArrived {
                peer,
                edge_id,
                stream_id: current_stream_id,
            });
            let mut chunk = vec![0u8; 4096];
            loop {
                match recv.read(&mut chunk).await {
                    Ok(Some(0)) | Ok(None) => {
                        events.lock().push(EdgeTransportEvent::StreamEnded {
                            peer,
                            edge_id,
                            stream_id: current_stream_id,
                        });
                        break;
                    }
                    Ok(Some(n)) => {
                        events.lock().push(EdgeTransportEvent::BytesRead {
                            peer,
                            edge_id,
                            stream_id: current_stream_id,
                            bytes: chunk[..n].to_vec(),
                        });
                    }
                    Err(_) => {
                        events.lock().push(EdgeTransportEvent::StreamFault {
                            peer,
                            edge_id: Some(edge_id),
                            stream_id: Some(current_stream_id),
                            reason: EdgeTransportFault::ReadError,
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
