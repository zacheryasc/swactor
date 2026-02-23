use std::sync::Arc;

use iroh::endpoint::Connection;

use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;

use crate::messages::{OneShot, StreamManagerMsg};
use crate::wire;

/// Spawn a bridge task that processes incoming stream connections and
/// forwards them to the StreamManager actor.
///
/// For each `(node_id, conn)` received:
/// 1. Accept the control bi-stream
/// 2. Read the stream header
/// 3. Send `StreamManagerMsg::IncomingConnection` to the StreamManager
pub fn spawn_accept_bridge(
    accepted_rx: tokio::sync::mpsc::Receiver<([u8; 32], Connection)>,
    runtime: Arc<Runtime>,
    manager_addr: ActorAddress,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        accept_bridge_loop(accepted_rx, runtime, manager_addr).await;
    })
}

async fn accept_bridge_loop(
    mut accepted_rx: tokio::sync::mpsc::Receiver<([u8; 32], Connection)>,
    runtime: Arc<Runtime>,
    manager_addr: ActorAddress,
) {
    while let Some((node_id, conn)) = accepted_rx.recv().await {
        let runtime = Arc::clone(&runtime);
        let manager_addr = manager_addr;
        tokio::spawn(async move {
            if let Err(e) = handle_incoming(node_id, conn, &runtime, manager_addr).await
            {
                eprintln!("accept bridge: failed to handle incoming connection: {e}");
            }
        });
    }
}

/// Handle a single incoming stream connection: accept the control bi-stream,
/// read the header, and forward to the StreamManager.
pub async fn handle_incoming(
    node_id: [u8; 32],
    conn: Connection,
    runtime: &Runtime,
    manager_addr: ActorAddress,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Accept the control bi-stream (opener sends header here)
    let (_, mut recv_ctrl) = conn.accept_bi().await?;

    // Read the full header into a buffer. The opener finishes the send side
    // after writing the header, so read_to_end collects all header bytes.
    let header_bytes = recv_ctrl.read_to_end(4096).await?;

    let header = wire::decode_header(&header_bytes)
        .map_err(|e| format!("invalid stream header: {e}"))?;

    let msg = StreamManagerMsg::IncomingConnection {
        node_id,
        stream_id: header.stream_id,
        mode: header.mode,
        config: header.config,
        conn: OneShot::new(conn),
    };
    runtime
        .send_to(manager_addr, msg)
        .map_err(|e| format!("send to StreamManager failed: {e}"))?;
    Ok(())
}
