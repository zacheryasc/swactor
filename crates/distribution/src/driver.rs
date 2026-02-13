//! Network driver — bridges `DistributedNode` logic with TCP I/O.
//!
//! Translates outgoing `NodeAction`s into wire messages sent via `TcpTransport`,
//! and dispatches incoming wire messages to the appropriate `DistributedNode`
//! handler methods.

use std::net::{SocketAddr, TcpStream};

use swactor::actor::ActorAddress;
use swactor::transport::{NetworkMessage, WireEnvelope};

use crate::messages::*;
use crate::node::{DistributedNode, DistributedNodeConfig};
use crate::snapshot::DistributionNodeSnapshot;
use crate::swim::node::NodeAction;
use crate::transport::{TcpAcceptor, TcpTransport};
use crate::types::NodeId;

/// Dummy destination address used in wire envelopes for SWIM protocol messages.
/// SWIM messages are routed by `SocketAddr`, not by `ActorAddress`, so this
/// field is unused but required by the wire format.
const SWIM_DEST: ActorAddress = ActorAddress([0u8; 32]);

/// Network driver that owns a `DistributedNode` and performs real TCP I/O.
pub struct NodeDriver {
    node: DistributedNode,
    transport: TcpTransport,
    acceptor: TcpAcceptor,
    streams: Vec<TcpStream>,
}

impl NodeDriver {
    /// Create a new driver. Binds a TCP listener on the node's `listen_addr`.
    pub fn new(config: DistributedNodeConfig) -> Result<Self, swactor::Error> {
        let listen_addr = config.listen_addr;
        let acceptor = TcpAcceptor::bind(listen_addr)?;
        let node = DistributedNode::new(config);
        Ok(Self {
            node,
            transport: TcpTransport::pool(),
            acceptor,
            streams: Vec::new(),
        })
    }

    /// The node's identity.
    pub fn node_id(&self) -> NodeId {
        self.node.node_id()
    }

    /// The address this driver is listening on.
    pub fn listen_addr(&self) -> SocketAddr {
        self.acceptor.local_addr()
    }

    /// Access the underlying node (read-only).
    pub fn node(&self) -> &DistributedNode {
        &self.node
    }

    /// Access the underlying node (mutable).
    pub fn node_mut(&mut self) -> &mut DistributedNode {
        &mut self.node
    }

    /// Capture a snapshot of the node's state.
    pub fn snapshot(&self) -> DistributionNodeSnapshot {
        self.node.snapshot()
    }

    /// Join a cluster by contacting seed nodes.
    ///
    /// Sends `JoinRequest` messages to each seed over TCP.
    pub fn join(&mut self, seeds: &[SocketAddr]) {
        let actions = self.node.join(seeds);
        self.send_actions(&actions);
    }

    /// Advance the node by one tick.
    ///
    /// Drives the SWIM probe cycle, sends outgoing protocol messages,
    /// and handles periodic republishing.
    pub fn tick(&mut self) {
        let actions = self.node.tick();
        self.send_actions(&actions);
    }

    /// Process incoming TCP messages.
    ///
    /// Reads all available wire envelopes from the acceptor, dispatches
    /// each to the appropriate handler, and sends any response actions.
    pub fn recv(&mut self) {
        let envelopes = self.acceptor.try_recv(&mut self.streams);
        for (envelope, _peer_addr) in envelopes {
            let response_actions = self.dispatch_incoming(envelope);
            self.send_actions(&response_actions);
        }
    }

    // ─── Outgoing: NodeAction → TCP ─────────────────────────────────────

    fn send_actions(&mut self, actions: &[NodeAction]) {
        for action in actions {
            if let Err(e) = self.send_action(action) {
                eprintln!("driver: send error: {e}");
            }
        }
    }

    fn send_action(&mut self, action: &NodeAction) -> Result<(), swactor::Error> {
        match action {
            NodeAction::SendPing {
                to_addr,
                sequence,
                piggyback,
                ..
            } => {
                let msg = Ping {
                    from: self.node.node_id(),
                    from_addr: self.node.listen_addr(),
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_wire::<Ping>(&msg, *to_addr)
            }

            NodeAction::SendAck {
                to_addr,
                sequence,
                piggyback,
                ..
            } => {
                let msg = Ack {
                    from: self.node.node_id(),
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_wire::<Ack>(&msg, *to_addr)
            }

            NodeAction::SendPingReq {
                relay_addr,
                target,
                target_addr,
                sequence,
                piggyback,
                ..
            } => {
                let msg = PingReq {
                    from: self.node.node_id(),
                    target: *target,
                    target_addr: *target_addr,
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_wire::<PingReq>(&msg, *relay_addr)
            }

            NodeAction::SendJoinRequest { to_addr } => {
                let msg = JoinRequest {
                    from: self.node.node_id(),
                    addr: self.node.listen_addr(),
                };
                self.send_wire::<JoinRequest>(&msg, *to_addr)
            }

            NodeAction::SendJoinResponse {
                to_addr, members, ..
            } => {
                let msg = JoinResponse {
                    members: members.clone(),
                };
                self.send_wire::<JoinResponse>(&msg, *to_addr)
            }

            NodeAction::MembershipChanged { .. } => {
                // Internal notification — no network I/O.
                Ok(())
            }
        }
    }

    fn send_wire<M: NetworkMessage + serde::Serialize>(
        &mut self,
        msg: &M,
        dest_addr: SocketAddr,
    ) -> Result<(), swactor::Error> {
        let payload = serde_json::to_vec(msg)
            .map_err(|e| swactor::Error::from(format!("encode {}: {e}", M::type_tag())))?;
        let envelope = WireEnvelope {
            dest: SWIM_DEST,
            type_tag: M::type_tag().to_string(),
            payload,
        };
        self.transport.send_to(dest_addr, envelope)
    }

    // ─── Incoming: TCP → handler ────────────────────────────────────────

    fn dispatch_incoming(&mut self, envelope: WireEnvelope) -> Vec<NodeAction> {
        match envelope.type_tag.as_str() {
            "swactor_dist::Ping" => match decode::<Ping>(&envelope.payload) {
                Ok(msg) => self.node.handle_ping(
                    msg.from,
                    msg.from_addr,
                    msg.sequence,
                    &msg.piggyback,
                ),
                Err(e) => {
                    eprintln!("driver: decode Ping: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::Ack" => match decode::<Ack>(&envelope.payload) {
                Ok(msg) => self.node.handle_ack(msg.from, msg.sequence, &msg.piggyback),
                Err(e) => {
                    eprintln!("driver: decode Ack: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::PingReq" => match decode::<PingReq>(&envelope.payload) {
                Ok(msg) => self.node.handle_ping_req(
                    msg.from,
                    msg.target,
                    msg.target_addr,
                    msg.sequence,
                    &msg.piggyback,
                ),
                Err(e) => {
                    eprintln!("driver: decode PingReq: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::JoinRequest" => match decode::<JoinRequest>(&envelope.payload) {
                Ok(msg) => self.node.handle_join_request(msg.from, msg.addr),
                Err(e) => {
                    eprintln!("driver: decode JoinRequest: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::JoinResponse" => match decode::<JoinResponse>(&envelope.payload) {
                Ok(msg) => self.node.handle_join_response(msg.members),
                Err(e) => {
                    eprintln!("driver: decode JoinResponse: {e}");
                    Vec::new()
                }
            },

            other => {
                eprintln!("driver: unknown message type: {other}");
                Vec::new()
            }
        }
    }
}

fn decode<M: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<M, String> {
    serde_json::from_slice(bytes).map_err(|e| e.to_string())
}
