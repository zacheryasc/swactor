use std::collections::HashMap;
use std::sync::Arc;

use iroh::{Endpoint, PublicKey};
use tokio::io::AsyncWriteExt;

use swactor::actor::{ActorAddress, ActorInterface, Ctx, Down};
use swactor::runtime::Runtime;

use crate::streams::data_plane;
use crate::streams::handle::{create_stream_handle, StreamHandle};
use crate::streams::messages::{OneShot, StreamManagerMsg, StreamNotification};
use crate::streams::types::{StreamConfig, StreamError, StreamId, StreamMode};
use crate::streams::wire;

/// Well-known name for the StreamManager actor in the name registry.
pub const STREAM_MANAGER_NAME: &str = "StreamManager";

/// Accept byte sent back on control stream to indicate stream acceptance.
const ACCEPT_BYTE: u8 = 0x01;
/// Reject byte sent back on control stream to indicate stream rejection.
const REJECT_BYTE: u8 = 0x00;

struct StreamState {
    owner: ActorAddress,
}

struct PendingIncoming {
    config: StreamConfig,
    conn: OneShot<iroh::endpoint::Connection>,
}

pub struct StreamManager {
    streams: HashMap<StreamId, StreamState>,
    pending_incoming: HashMap<StreamId, PendingIncoming>,
    listeners: HashMap<StreamMode, Vec<ActorAddress>>,
    endpoint: Endpoint,
    tokio_handle: tokio::runtime::Handle,
    runtime: Arc<Runtime>,
    self_addr: Option<ActorAddress>,
}

impl StreamManager {
    pub fn new(
        endpoint: Endpoint,
        tokio_handle: tokio::runtime::Handle,
        runtime: Arc<Runtime>,
    ) -> Self {
        StreamManager {
            streams: HashMap::new(),
            pending_incoming: HashMap::new(),
            listeners: HashMap::new(),
            endpoint,
            tokio_handle,
            runtime,
            self_addr: None,
        }
    }

    fn handle_open(
        &mut self,
        target_node: [u8; 32],
        mode: StreamMode,
        config: StreamConfig,
        reply_to: ActorAddress,
    ) {
        let stream_id = StreamId::new_random();
        let endpoint = self.endpoint.clone();
        let runtime = Arc::clone(&self.runtime);
        let self_addr = self.self_addr.expect("StreamManager not started");

        self.tokio_handle.spawn(async move {
            let result = open_stream_async(endpoint, target_node, stream_id, mode, &config).await;
            let msg = StreamManagerMsg::OpenCompleted {
                stream_id,
                reply_to,
                result: OneShot::new(result),
            };
            let _ = runtime.send_to(self_addr, msg);
        });
    }

    fn handle_open_completed(
        &mut self,
        ctx: &Ctx,
        stream_id: StreamId,
        reply_to: ActorAddress,
        result: OneShot<Result<StreamHandle, StreamError>>,
    ) {
        match result.take() {
            Some(Ok(handle)) => {
                self.streams.insert(
                    stream_id,
                    StreamState {
                        owner: reply_to,
                    },
                );
                let notif = StreamNotification::StreamReady {
                    stream_id,
                    handle: OneShot::new(handle),
                };
                let _ = ctx.send(reply_to, notif);
            }
            Some(Err(err)) => {
                let notif = StreamNotification::StreamFailed {
                    stream_id,
                    error: err,
                };
                let _ = ctx.send(reply_to, notif);
            }
            None => {
                // OneShot already consumed — should not happen
                eprintln!("StreamManager: OpenCompleted result already consumed");
            }
        }
    }

    fn handle_incoming_connection(
        &mut self,
        ctx: &Ctx,
        node_id: [u8; 32],
        stream_id: StreamId,
        mode: StreamMode,
        config: StreamConfig,
        conn: OneShot<iroh::endpoint::Connection>,
    ) {
        // Notify listeners for this mode
        if let Some(listeners) = self.listeners.get(&mode) {
            let notif = StreamNotification::StreamOffer {
                stream_id,
                mode,
                metadata: config.metadata.clone(),
                from_node: node_id,
            };
            for listener in listeners {
                let _ = ctx.send(*listener, notif.clone());
            }
        }

        // Store pending incoming for Accept/Reject
        self.pending_incoming.insert(
            stream_id,
            PendingIncoming {
                config,
                conn,
            },
        );
    }

    fn handle_accept(&mut self, stream_id: StreamId, reply_to: ActorAddress) {
        let pending = match self.pending_incoming.remove(&stream_id) {
            Some(p) => p,
            None => {
                eprintln!("StreamManager: Accept for unknown stream {stream_id}");
                return;
            }
        };

        let conn = match pending.conn.take() {
            Some(c) => c,
            None => {
                eprintln!("StreamManager: Accept connection already consumed for {stream_id}");
                return;
            }
        };

        let config = pending.config;
        let runtime = Arc::clone(&self.runtime);
        let self_addr = self.self_addr.expect("StreamManager not started");

        self.tokio_handle.spawn(async move {
            let result = accept_stream_async(conn, stream_id, &config).await;
            let msg = StreamManagerMsg::AcceptCompleted {
                stream_id,
                reply_to,
                result: OneShot::new(result),
            };
            let _ = runtime.send_to(self_addr, msg);
        });
    }

    fn handle_accept_completed(
        &mut self,
        ctx: &Ctx,
        stream_id: StreamId,
        reply_to: ActorAddress,
        result: OneShot<Result<StreamHandle, StreamError>>,
    ) {
        match result.take() {
            Some(Ok(handle)) => {
                self.streams.insert(
                    stream_id,
                    StreamState {
                        owner: reply_to,
                    },
                );
                let notif = StreamNotification::StreamReady {
                    stream_id,
                    handle: OneShot::new(handle),
                };
                let _ = ctx.send(reply_to, notif);
            }
            Some(Err(err)) => {
                let notif = StreamNotification::StreamFailed {
                    stream_id,
                    error: err,
                };
                let _ = ctx.send(reply_to, notif);
            }
            None => {
                eprintln!("StreamManager: AcceptCompleted result already consumed");
            }
        }
    }

    fn handle_reject(&mut self, stream_id: StreamId) {
        if let Some(pending) = self.pending_incoming.remove(&stream_id) {
            // If we have the connection, send reject and close
            if let Some(conn) = pending.conn.take() {
                let runtime = Arc::clone(&self.runtime);
                self.tokio_handle.spawn(async move {
                    // Best-effort: send reject on any open bi-stream, then close
                    let _ = reject_stream_async(&conn).await;
                    drop(conn);
                    drop(runtime);
                });
            }
        }
    }

    fn handle_listen(&mut self, mode: StreamMode, listener: ActorAddress) {
        self.listeners
            .entry(mode)
            .or_default()
            .push(listener);
    }

    fn handle_close(&mut self, stream_id: StreamId) {
        // Remove stream state; data-plane tasks terminate when channels drop
        self.streams.remove(&stream_id);
    }
}

impl ActorInterface for StreamManager {
    type Incoming = StreamManagerMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.self_addr = Some(ctx.self_addr());
    }

    fn handle(&mut self, ctx: &Ctx, msg: StreamManagerMsg) {
        match msg {
            StreamManagerMsg::Open {
                target_node,
                mode,
                config,
                reply_to,
            } => self.handle_open(target_node, mode, config, reply_to),

            StreamManagerMsg::Accept {
                stream_id,
                reply_to,
            } => self.handle_accept(stream_id, reply_to),

            StreamManagerMsg::Reject { stream_id } => self.handle_reject(stream_id),

            StreamManagerMsg::Listen { mode, listener } => self.handle_listen(mode, listener),

            StreamManagerMsg::Close { stream_id } => self.handle_close(stream_id),

            StreamManagerMsg::IncomingConnection {
                node_id,
                stream_id,
                mode,
                config,
                conn,
            } => self.handle_incoming_connection(ctx, node_id, stream_id, mode, config, conn),

            StreamManagerMsg::OpenCompleted {
                stream_id,
                reply_to,
                result,
            } => self.handle_open_completed(ctx, stream_id, reply_to, result),

            StreamManagerMsg::AcceptCompleted {
                stream_id,
                reply_to,
                result,
            } => self.handle_accept_completed(ctx, stream_id, reply_to, result),
        }
    }

    fn handle_down(&mut self, _ctx: &Ctx, down: Down) {
        // Clean up streams owned by the dead actor
        let dead_addr = down.addr;
        self.streams.retain(|_, state| state.owner != dead_addr);

        // Remove from listeners
        for listeners in self.listeners.values_mut() {
            listeners.retain(|addr| *addr != dead_addr);
        }
    }
}

// ─── Async helpers (run inside tokio tasks) ─────────────────────────────

/// Open a stream to a remote node: connect, send header on control bi-stream,
/// wait for accept/reject, then spawn data-plane tasks.
async fn open_stream_async(
    endpoint: Endpoint,
    target_node: [u8; 32],
    stream_id: StreamId,
    mode: StreamMode,
    config: &StreamConfig,
) -> Result<StreamHandle, StreamError> {
    let key = PublicKey::from_bytes(&target_node)
        .map_err(|e| StreamError::BrokenPipe(format!("invalid public key: {e}")))?;

    let conn = endpoint
        .connect(key, wire::ALPN)
        .await
        .map_err(|e| StreamError::BrokenPipe(format!("connect failed: {e}")))?;

    // Open control bi-stream and send header
    let (mut send_ctrl, _recv_ctrl) = conn
        .open_bi()
        .await
        .map_err(|e| StreamError::BrokenPipe(format!("open_bi failed: {e}")))?;

    let header = wire::StreamHeader {
        stream_id,
        mode,
        config: config.clone(),
    };
    let header_bytes = wire::encode_header(&header);
    send_ctrl
        .write_all(&header_bytes)
        .await
        .map_err(|e| StreamError::BrokenPipe(format!("write header failed: {e}")))?;
    send_ctrl
        .finish()
        .map_err(|e| StreamError::BrokenPipe(format!("finish control send failed: {e}")))?;

    // Wait for accept/reject response on a uni-stream opened by the acceptor.
    // (The bi-stream's send half was dropped by the accept bridge after reading
    // the header, so the acceptor responds via a separate uni-stream.)
    let mut response_recv = conn
        .accept_uni()
        .await
        .map_err(|e| StreamError::BrokenPipe(format!("accept response stream failed: {e}")))?;
    let mut response = [0u8; 1];
    response_recv
        .read_exact(&mut response)
        .await
        .map_err(|e| StreamError::BrokenPipe(format!("read accept/reject failed: {e}")))?;

    if response[0] != ACCEPT_BYTE {
        return Err(StreamError::BrokenPipe("stream rejected by remote".into()));
    }

    // Create StreamHandle and spawn data-plane tasks
    let stripe_count = config.stripe_count as usize;
    let (handle, endpoints) = create_stream_handle(stream_id, config, 32, 16);

    // Spawn send stripe tasks with QUIC uni-streams
    {
        let pool = endpoints.pool.clone();
        let mut cmd_rx = endpoints.send_cmd_rx;
        let evt_tx = endpoints.send_evt_tx;
        let conn_clone = conn.clone();
        let sc = stripe_count;

        tokio::spawn(async move {
            // Open uni-streams for each stripe
            let mut writers = Vec::with_capacity(sc);
            for _ in 0..sc {
                match conn_clone.open_uni().await {
                    Ok(send_stream) => writers.push(send_stream),
                    Err(e) => {
                        let _ = evt_tx
                            .send(crate::streams::channel::SendEvent::Error(StreamError::BrokenPipe(
                                format!("open_uni failed: {e}"),
                            )))
                            .await;
                        return;
                    }
                }
            }

            // Simple single-task approach: round-robin commands across stripes
            let mut stripe_idx = 0;
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    crate::streams::channel::SendCommand::Data(buf) => {
                        let frame = wire::encode_data_frame(buf.written());
                        let writer = &mut writers[stripe_idx];
                        if let Err(e) = writer.write_all(&frame).await {
                            pool.checkin(buf);
                            let _ = evt_tx
                                .send(crate::streams::channel::SendEvent::Error(
                                    StreamError::BrokenPipe(e.to_string()),
                                ))
                                .await;
                            return;
                        }
                        pool.checkin(buf);
                        stripe_idx = (stripe_idx + 1) % sc;
                    }
                    crate::streams::channel::SendCommand::Flush => {
                        for writer in &mut writers {
                            let _ = writer.flush().await;
                        }
                    }
                    crate::streams::channel::SendCommand::Close => {
                        let sentinel = wire::encode_end_of_stripe();
                        for writer in &mut writers {
                            let _ = writer.write_all(&sentinel).await;
                            let _ = writer.finish();
                        }
                        break;
                    }
                }
            }
        });
    }

    // Spawn recv stripe tasks with QUIC uni-streams (accepted from remote)
    {
        let pool = endpoints.pool.clone();
        let evt_tx = endpoints.recv_evt_tx;
        let conn_clone = conn.clone();
        let sc = stripe_count;

        tokio::spawn(async move {
            // Accept uni-streams for each recv stripe
            let mut closed_count = 0;
            loop {
                match conn_clone.accept_uni().await {
                    Ok(recv_stream) => {
                        let pool = pool.clone();
                        let tx = evt_tx.clone();
                        tokio::spawn(async move {
                            let _ =
                                data_plane::recv_stripe_task(recv_stream, tx, pool, None).await;
                        });
                        closed_count += 1;
                        if closed_count >= sc {
                            // We only expect stripe_count recv streams
                            // but keep accepting in case more arrive
                        }
                    }
                    Err(_) => break,
                }
            }
        });
    }

    Ok(handle)
}

/// Accept a stream: send accept byte on control stream, spawn data-plane tasks.
async fn accept_stream_async(
    conn: iroh::endpoint::Connection,
    stream_id: StreamId,
    config: &StreamConfig,
) -> Result<StreamHandle, StreamError> {
    // Open a uni-stream to send the accept byte back
    // (The opener reads from the recv side of the bi-stream they opened.
    //  We need to open our own bi-stream to send the response.)
    // Actually, the opener opened a bi-stream - we need to accept it and
    // respond on it. But IncomingConnection already accepted the bi-stream
    // and read the header. We need the send half of that bi-stream.
    //
    // Since the accept bridge consumed the bi-stream to read the header,
    // we send the accept response on a new uni-stream that the opener
    // will accept_uni on. But the plan says "1-byte accept/reject response"
    // on the same control bi-stream.
    //
    // The design: the accept bridge reads the header from the bi-stream
    // (recv side), and the StreamManager sends accept/reject on the
    // send side. Since the accept bridge consumed the Connection but not
    // the bi-stream send half, we need a different approach.
    //
    // Simpler: use a uni-stream for the response.
    let mut response_stream = conn
        .open_uni()
        .await
        .map_err(|e| StreamError::BrokenPipe(format!("open response stream failed: {e}")))?;
    response_stream
        .write_all(&[ACCEPT_BYTE])
        .await
        .map_err(|e| StreamError::BrokenPipe(format!("write accept byte failed: {e}")))?;
    response_stream
        .finish()
        .map_err(|e| StreamError::BrokenPipe(format!("finish response stream failed: {e}")))?;

    let stripe_count = config.stripe_count as usize;
    let (handle, endpoints) = create_stream_handle(stream_id, config, 32, 16);

    // Spawn send stripe tasks — we open uni-streams to write
    {
        let pool = endpoints.pool.clone();
        let mut cmd_rx = endpoints.send_cmd_rx;
        let evt_tx = endpoints.send_evt_tx;
        let conn_clone = conn.clone();
        let sc = stripe_count;

        tokio::spawn(async move {
            let mut writers = Vec::with_capacity(sc);
            for _ in 0..sc {
                match conn_clone.open_uni().await {
                    Ok(send_stream) => writers.push(send_stream),
                    Err(e) => {
                        let _ = evt_tx
                            .send(crate::streams::channel::SendEvent::Error(StreamError::BrokenPipe(
                                format!("open_uni failed: {e}"),
                            )))
                            .await;
                        return;
                    }
                }
            }

            let mut stripe_idx = 0;
            while let Some(cmd) = cmd_rx.recv().await {
                match cmd {
                    crate::streams::channel::SendCommand::Data(buf) => {
                        let frame = wire::encode_data_frame(buf.written());
                        let writer = &mut writers[stripe_idx];
                        if let Err(e) = writer.write_all(&frame).await {
                            pool.checkin(buf);
                            let _ = evt_tx
                                .send(crate::streams::channel::SendEvent::Error(
                                    StreamError::BrokenPipe(e.to_string()),
                                ))
                                .await;
                            return;
                        }
                        pool.checkin(buf);
                        stripe_idx = (stripe_idx + 1) % sc;
                    }
                    crate::streams::channel::SendCommand::Flush => {
                        for writer in &mut writers {
                            let _ = writer.flush().await;
                        }
                    }
                    crate::streams::channel::SendCommand::Close => {
                        let sentinel = wire::encode_end_of_stripe();
                        for writer in &mut writers {
                            let _ = writer.write_all(&sentinel).await;
                            let _ = writer.finish();
                        }
                        break;
                    }
                }
            }
        });
    }

    // Spawn recv stripe tasks — accept uni-streams from remote
    {
        let pool = endpoints.pool.clone();
        let evt_tx = endpoints.recv_evt_tx;
        let conn_clone = conn.clone();

        tokio::spawn(async move {
            loop {
                match conn_clone.accept_uni().await {
                    Ok(recv_stream) => {
                        let pool = pool.clone();
                        let tx = evt_tx.clone();
                        tokio::spawn(async move {
                            let _ =
                                data_plane::recv_stripe_task(recv_stream, tx, pool, None).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });
    }

    Ok(handle)
}

/// Send reject on a connection (best-effort).
async fn reject_stream_async(
    conn: &iroh::endpoint::Connection,
) -> Result<(), StreamError> {
    let mut response_stream = conn
        .open_uni()
        .await
        .map_err(|e| StreamError::BrokenPipe(format!("open response stream failed: {e}")))?;
    response_stream
        .write_all(&[REJECT_BYTE])
        .await
        .map_err(|e| StreamError::BrokenPipe(format!("write reject byte failed: {e}")))?;
    response_stream
        .finish()
        .map_err(|e| StreamError::BrokenPipe(format!("finish response stream failed: {e}")))?;
    Ok(())
}
