//! iroh-based P2P network driver for `DistributedNode`.
//!
//! Provides the same driver pattern as `NodeDriver` (TCP), but uses iroh's
//! QUIC-based peer-to-peer transport with built-in TLS, NAT hole-punching,
//! and relay server fallback.
//!
//! The driver owns a tokio runtime internally, exposing a synchronous API
//! (`tick()`, `recv()`, `join()`) to match the existing main loop pattern.

use std::collections::HashMap;
use std::time::Duration;

use iroh::endpoint::Connection;
use iroh::{Endpoint, PublicKey, RelayMode, SecretKey};
use tokio::runtime::Runtime as TokioRuntime;


use crate::crypto::Keypair;
use crate::messages::*;
use crate::node::{DistributedNode, DistributedNodeConfig};
use crate::snapshot::DistributionNodeSnapshot;
use crate::swim::node::NodeAction;
use crate::types::NodeId;

/// ALPN protocol identifier for SWIM messages over iroh.
const ALPN: &[u8] = b"swactor/swim/1";

// ─── Config ─────────────────────────────────────────────────────────────────

/// Configuration for the iroh-based driver.
pub struct IrohDriverConfig {
    /// Secret key for the iroh endpoint.
    /// If `None`, a fresh key is generated (node gets a random identity).
    pub secret_key: Option<SecretKey>,
    /// Relay server configuration.
    /// Defaults to `RelayMode::Default` (n0 production relays).
    pub relay_mode: RelayMode,
    /// Protocol-layer configuration.
    pub node: DistributedNodeConfig,
}

// ─── Driver ─────────────────────────────────────────────────────────────────

/// iroh P2P network driver.
///
/// Bridges the synchronous `DistributedNode` state machine with iroh's
/// async QUIC transport. Owns a tokio runtime internally.
pub struct IrohDriver {
    node: DistributedNode,
    endpoint: Endpoint,
    rt: TokioRuntime,
    connections: HashMap<NodeId, Connection>,
}

impl IrohDriver {
    /// Create a new iroh driver.
    ///
    /// Builds a tokio runtime, creates an iroh `Endpoint`, and initializes
    /// the protocol-layer `DistributedNode`.
    pub fn new(config: IrohDriverConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        let endpoint = rt.block_on(async {
            let mut builder = Endpoint::builder()
                .alpns(vec![ALPN.to_vec()])
                .relay_mode(config.relay_mode);

            if let Some(key) = config.secret_key {
                builder = builder.secret_key(key);
            }

            builder.bind().await
        })?;

        // Create a DistributedNode whose identity matches the iroh endpoint.
        // Both use ed25519-dalek, so we can reconstruct our Keypair from iroh's secret key.
        let iroh_secret = endpoint.secret_key().to_bytes();
        let keypair = Keypair::from_bytes(&iroh_secret);
        let node = DistributedNode::with_keypair(keypair, config.node);

        Ok(Self {
            node,
            endpoint,
            rt,
            connections: HashMap::new(),
        })
    }

    /// The node's identity.
    pub fn node_id(&self) -> NodeId {
        self.node.node_id()
    }

    /// Access the underlying node (read-only).
    pub fn node(&self) -> &DistributedNode {
        &self.node
    }

    /// Access the underlying node (mutable).
    pub fn node_mut(&mut self) -> &mut DistributedNode {
        &mut self.node
    }

    /// Capture a snapshot enriched with iroh endpoint info.
    pub fn snapshot(&self) -> DistributionNodeSnapshot {
        let mut snap = self.node.snapshot();
        // Use iroh endpoint address as the "listen address"
        let addr_info = self.rt.block_on(async {
            format!("{}", self.endpoint.id())
        });
        snap.listen_addr = Some(addr_info);
        snap
    }

    /// Join a cluster by connecting to seed nodes via iroh.
    ///
    /// Each seed is identified by its iroh `PublicKey` (= our `NodeId`).
    pub fn join(&mut self, seeds: &[PublicKey]) {
        for seed_key in seeds {
            let seed_node_id = NodeId(*seed_key.as_bytes());
            if let Err(e) = self.send_join_request(*seed_key, seed_node_id) {
                eprintln!("iroh driver: join error to {seed_key}: {e}");
            }
        }
    }

    fn send_join_request(
        &mut self,
        seed_key: PublicKey,
        seed_node_id: NodeId,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let msg = JoinRequest {
            from: self.node.node_id(),
        };
        let payload = serde_json::to_vec(&msg)?;
        let tag = <JoinRequest as swactor::transport::NetworkMessage>::type_tag();

        let endpoint = self.endpoint.clone();
        let conn = self.rt.block_on(async {
            let conn = endpoint.connect(seed_key, ALPN).await?;
            let mut send = conn.open_uni().await?;
            write_message(&mut send, tag.as_bytes(), &payload).await?;
            send.finish()?;
            Ok::<_, Box<dyn std::error::Error>>(conn)
        })?;

        self.connections.insert(seed_node_id, conn);
        Ok(())
    }

    /// Advance the node by one tick.
    pub fn tick(&mut self) {
        let actions = self.node.tick();
        self.send_actions(&actions);
    }

    /// Process incoming iroh connections and messages (non-blocking).
    pub fn recv(&mut self) {
        let (incoming, new_conns) = self.rt.block_on(async {
            self.receive_pending().await
        });
        // Cache connections accepted from remote peers
        for (node_id, conn) in new_conns {
            self.connections.entry(node_id).or_insert(conn);
        }
        for (tag, payload, from_key) in incoming {
            let from = NodeId(*from_key.as_bytes());
            let response_actions = self.dispatch_incoming(&tag, &payload, from);
            self.send_actions(&response_actions);
        }
    }

    // ─── Outgoing: NodeAction → iroh ─────────────────────────────────

    fn send_actions(&mut self, actions: &[NodeAction]) {
        for action in actions {
            if let Err(e) = self.send_action(action) {
                eprintln!("iroh driver: send error: {e}");
            }
        }
    }

    fn send_action(&mut self, action: &NodeAction) -> Result<(), Box<dyn std::error::Error>> {
        match action {
            NodeAction::SendPing {
                to,
                sequence,
                piggyback,
            } => {
                let msg = Ping {
                    from: self.node.node_id(),
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_message(to, &msg)
            }

            NodeAction::SendAck {
                to,
                sequence,
                piggyback,
            } => {
                let msg = Ack {
                    from: self.node.node_id(),
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_message(to, &msg)
            }

            NodeAction::SendPingReq {
                relay,
                target,
                sequence,
                piggyback,
            } => {
                let msg = PingReq {
                    from: self.node.node_id(),
                    target: *target,
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_message(relay, &msg)
            }

            NodeAction::SendJoinResponse { to, members } => {
                let msg = JoinResponse {
                    members: members.clone(),
                };
                self.send_message(to, &msg)
            }

            NodeAction::MembershipChanged { .. } => Ok(()),
        }
    }

    fn send_message<M: swactor::transport::NetworkMessage + serde::Serialize>(
        &mut self,
        to: &NodeId,
        msg: &M,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tag = M::type_tag();
        let payload = serde_json::to_vec(msg)?;
        let target_key = PublicKey::from_bytes(&to.0)?;

        let conn = self.get_or_connect(*to, target_key)?;

        let result = self.rt.block_on(async {
            let mut send = conn.open_uni().await?;
            write_message(&mut send, tag.as_bytes(), &payload).await?;
            send.finish()?;
            Ok::<_, Box<dyn std::error::Error>>(())
        });

        if let Err(_) = result {
            // Connection may be stale, remove and retry once
            self.connections.remove(to);
            let conn = self.get_or_connect(*to, target_key)?;
            self.rt.block_on(async {
                let mut send = conn.open_uni().await?;
                write_message(&mut send, tag.as_bytes(), &payload).await?;
                send.finish()?;
                Ok::<_, Box<dyn std::error::Error>>(())
            })?;
        }

        Ok(())
    }

    fn get_or_connect(
        &mut self,
        node_id: NodeId,
        key: PublicKey,
    ) -> Result<Connection, Box<dyn std::error::Error>> {
        // Check for cached connection that's still open
        if let Some(conn) = self.connections.get(&node_id) {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
            // Connection closed, remove it
            self.connections.remove(&node_id);
        }

        let endpoint = self.endpoint.clone();
        let conn = self.rt.block_on(async {
            endpoint.connect(key, ALPN).await
        })?;

        self.connections.insert(node_id, conn.clone());
        Ok(conn)
    }

    // ─── Incoming: iroh → handler ────────────────────────────────────

    async fn receive_pending(&self) -> (Vec<(String, Vec<u8>, PublicKey)>, Vec<(NodeId, Connection)>) {
        let mut messages = Vec::new();
        let mut new_connections = Vec::new();

        // Poll for incoming connections with a short timeout
        loop {
            let accept_fut = self.endpoint.accept();
            let result = tokio::time::timeout(Duration::from_millis(1), accept_fut).await;

            match result {
                Ok(Some(incoming)) => {
                    if let Ok(conn) = incoming.await {
                        let remote_id = conn.remote_id();
                        self.read_streams(&conn, remote_id, &mut messages).await;
                        let node_id = NodeId(*remote_id.as_bytes());
                        new_connections.push((node_id, conn));
                    }
                }
                _ => break,
            }
        }

        // Also read from existing cached connections
        let conn_snapshot: Vec<(NodeId, Connection)> = self
            .connections
            .iter()
            .map(|(id, c)| (*id, c.clone()))
            .collect();

        for (node_id, conn) in conn_snapshot {
            let remote_id = PublicKey::from_bytes(&node_id.0).unwrap();
            self.read_streams(&conn, remote_id, &mut messages).await;
        }

        (messages, new_connections)
    }

    async fn read_streams(
        &self,
        conn: &Connection,
        remote_id: PublicKey,
        messages: &mut Vec<(String, Vec<u8>, PublicKey)>,
    ) {
        loop {
            match tokio::time::timeout(Duration::from_millis(1), conn.accept_uni()).await {
                Ok(Ok(mut recv)) => {
                    match read_message(&mut recv).await {
                        Ok((tag, payload)) => {
                            messages.push((tag, payload, remote_id));
                        }
                        Err(e) => {
                            eprintln!("iroh driver: read error: {e}");
                            break;
                        }
                    }
                }
                _ => break,
            }
        }
    }

    fn dispatch_incoming(
        &mut self,
        tag: &str,
        payload: &[u8],
        _from: NodeId,
    ) -> Vec<NodeAction> {
        match tag {
            "swactor_dist::Ping" => match serde_json::from_slice::<Ping>(payload) {
                Ok(msg) => self.node.handle_ping(msg.from, msg.sequence, &msg.piggyback),
                Err(e) => {
                    eprintln!("iroh driver: decode Ping: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::Ack" => match serde_json::from_slice::<Ack>(payload) {
                Ok(msg) => self.node.handle_ack(msg.from, msg.sequence, &msg.piggyback),
                Err(e) => {
                    eprintln!("iroh driver: decode Ack: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::PingReq" => match serde_json::from_slice::<PingReq>(payload) {
                Ok(msg) => {
                    self.node
                        .handle_ping_req(msg.from, msg.target, msg.sequence, &msg.piggyback)
                }
                Err(e) => {
                    eprintln!("iroh driver: decode PingReq: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::JoinRequest" => {
                match serde_json::from_slice::<JoinRequest>(payload) {
                    Ok(msg) => self.node.handle_join_request(msg.from),
                    Err(e) => {
                        eprintln!("iroh driver: decode JoinRequest: {e}");
                        Vec::new()
                    }
                }
            }

            "swactor_dist::JoinResponse" => {
                match serde_json::from_slice::<JoinResponse>(payload) {
                    Ok(msg) => self.node.handle_join_response(msg.members),
                    Err(e) => {
                        eprintln!("iroh driver: decode JoinResponse: {e}");
                        Vec::new()
                    }
                }
            }

            other => {
                eprintln!("iroh driver: unknown message type: {other}");
                Vec::new()
            }
        }
    }

    /// Shut down the iroh endpoint.
    pub fn shutdown(&self) {
        self.rt.block_on(async {
            self.endpoint.close().await;
        });
    }
}

// ─── Wire Framing Over QUIC Streams ─────────────────────────────────────────

/// Write a tagged message to a QUIC send stream.
///
/// Frame format: `[4B tag_len][tag_bytes][payload_bytes]`
async fn write_message(
    send: &mut iroh::endpoint::SendStream,
    tag: &[u8],
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let tag_len = (tag.len() as u32).to_be_bytes();
    send.write_all(&tag_len).await?;
    send.write_all(tag).await?;
    send.write_all(payload).await?;
    Ok(())
}

/// Read a tagged message from a QUIC recv stream.
///
/// Returns `(type_tag, payload)`.
async fn read_message(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<(String, Vec<u8>), Box<dyn std::error::Error>> {
    let mut tag_len_buf = [0u8; 4];
    recv.read_exact(&mut tag_len_buf).await?;
    let tag_len = u32::from_be_bytes(tag_len_buf) as usize;

    if tag_len > 1024 {
        return Err("tag too large".into());
    }

    let mut tag_buf = vec![0u8; tag_len];
    recv.read_exact(&mut tag_buf).await?;
    let tag = String::from_utf8(tag_buf)?;

    let payload = recv.read_to_end(64 * 1024).await?;

    Ok((tag, payload))
}
