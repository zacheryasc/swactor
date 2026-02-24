//! Network driver — bridges `DistributedNode` logic with TCP I/O.
//!
//! Translates outgoing `NodeAction`s into wire messages sent via `TcpTransport`,
//! and dispatches incoming wire messages to the appropriate `DistributedNode`
//! handler methods.
//!
//! The driver maintains a `PeerAddressBook` mapping `NodeId → SocketAddr`.
//! Address hints travel in the TCP wire frame (not in protocol messages),
//! keeping the protocol layer transport-agnostic.

use std::collections::HashMap;
use std::net::{SocketAddr, TcpStream};

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor::transport::{NetworkMessage, WireEnvelope};

use crate::messages::*;
use crate::node::{DistributedNode, DistributedNodeConfig};
use crate::snapshot::DistributionNodeSnapshot;
use crate::swim::node::NodeAction;
use swactor_transport::tcp::{TcpAcceptor, TcpTransport, encode_wire_envelope_with_hints};
use crate::types::NodeId;

/// Dummy destination address used in wire envelopes for SWIM protocol messages.
/// SWIM messages are routed by `SocketAddr`, not by `ActorAddress`, so this
/// field is unused but required by the wire format.
const SWIM_DEST: ActorAddress = ActorAddress([0u8; 32]);

// ─── Address Hints ──────────────────────────────────────────────────────────

/// An address hint bundled in the TCP wire frame.
///
/// Each outgoing TCP message includes the sender's own (NodeId, SocketAddr)
/// as a hint. JoinResponse messages include all known member addresses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddressHint {
    pub node_id: NodeId,
    pub addr: SocketAddr,
}

// ─── Peer Address Book ──────────────────────────────────────────────────────

/// Maps NodeId → SocketAddr. Maintained by the TCP driver layer.
pub struct PeerAddressBook(HashMap<NodeId, SocketAddr>);

impl Default for PeerAddressBook {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerAddressBook {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    /// Learn a node's address from a hint.
    pub fn learn(&mut self, node_id: NodeId, addr: SocketAddr) {
        self.0.insert(node_id, addr);
    }

    /// Resolve a node's address.
    pub fn resolve(&self, node_id: &NodeId) -> Option<SocketAddr> {
        self.0.get(node_id).copied()
    }

    /// Learn multiple hints at once.
    pub fn bulk_learn(&mut self, hints: &[AddressHint]) {
        for hint in hints {
            self.learn(hint.node_id, hint.addr);
        }
    }
}

// ─── NodeDriver ─────────────────────────────────────────────────────────────

/// Network driver that owns a `DistributedNode` and performs real TCP I/O.
pub struct NodeDriver {
    node: DistributedNode,
    transport: TcpTransport,
    acceptor: TcpAcceptor,
    streams: Vec<TcpStream>,
    address_book: PeerAddressBook,
    listen_addr: SocketAddr,
}

impl NodeDriver {
    /// Create a new driver. Binds a TCP listener on `listen_addr`.
    pub fn new(listen_addr: SocketAddr, config: DistributedNodeConfig) -> Result<Self, swactor::Error> {
        let acceptor = TcpAcceptor::bind(listen_addr)?;
        let actual_addr = acceptor.local_addr();
        let node = DistributedNode::new(config);
        Ok(Self {
            node,
            transport: TcpTransport::pool(),
            acceptor,
            streams: Vec::new(),
            address_book: PeerAddressBook::new(),
            listen_addr: actual_addr,
        })
    }

    /// Create a driver with a specific keypair for persistent identity.
    pub fn with_keypair(
        listen_addr: SocketAddr,
        keypair: crate::crypto::Keypair,
        config: DistributedNodeConfig,
    ) -> Result<Self, swactor::Error> {
        let acceptor = TcpAcceptor::bind(listen_addr)?;
        let actual_addr = acceptor.local_addr();
        let node = DistributedNode::with_keypair(keypair, config);
        Ok(Self {
            node,
            transport: TcpTransport::pool(),
            acceptor,
            streams: Vec::new(),
            address_book: PeerAddressBook::new(),
            listen_addr: actual_addr,
        })
    }

    /// The node's identity.
    pub fn node_id(&self) -> NodeId {
        self.node.node_id()
    }

    /// The address this driver is listening on.
    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Access the underlying node (read-only).
    pub fn node(&self) -> &DistributedNode {
        &self.node
    }

    /// Access the underlying node (mutable).
    pub fn node_mut(&mut self) -> &mut DistributedNode {
        &mut self.node
    }

    /// Capture a snapshot of the node's state, enriched with addresses
    /// from the driver's address book.
    pub fn snapshot(&self) -> DistributionNodeSnapshot {
        let mut snap = self.node.snapshot();
        snap.listen_addr = Some(self.listen_addr.to_string());

        // Enrich member addresses from address book
        for member in &mut snap.members {
            if let Some(node_id) = parse_node_id_hex(&member.node_id)
                && let Some(addr) = self.address_book.resolve(&node_id) {
                    member.addr = Some(addr.to_string());
                }
        }

        // Enrich routing neighbor addresses from address book
        for neighbor in &mut snap.routing_neighbors {
            if let Some(node_id) = parse_node_id_hex(&neighbor.node_id)
                && let Some(addr) = self.address_book.resolve(&node_id) {
                    neighbor.addr = Some(addr.to_string());
                }
        }

        snap
    }

    /// Join a cluster by contacting seed nodes.
    ///
    /// Sends `JoinRequest` messages directly to each seed over TCP.
    /// On receiving a `JoinResponse`, extracts address hints and passes
    /// the member records to the protocol layer.
    pub fn join(&mut self, seeds: &[SocketAddr]) {
        for seed_addr in seeds {
            if let Err(e) = self.send_join_request(*seed_addr) {
                eprintln!("driver: join send error to {seed_addr}: {e}");
            }
        }
    }

    fn send_join_request(&mut self, seed_addr: SocketAddr) -> Result<(), swactor::Error> {
        let msg = JoinRequest {
            from: self.node.node_id(),
        };
        let hints = vec![AddressHint {
            node_id: self.node.node_id(),
            addr: self.listen_addr,
        }];
        self.send_wire_with_hints::<JoinRequest>(&msg, seed_addr, &hints)
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
        for (envelope, _peer_addr, hints_bytes) in envelopes {
            if !hints_bytes.is_empty()
                && let Ok(hints) = serde_json::from_slice::<Vec<AddressHint>>(&hints_bytes) {
                    self.learn_hints(&hints);
                }
            let response_actions = self.dispatch_incoming(envelope);
            self.send_actions(&response_actions);
        }
    }

    /// Process incoming TCP messages with peer auth filtering.
    ///
    /// Same as `recv()`, but checks the sender's NodeId against the
    /// peer allow-list before dispatching. Unauthorized messages are dropped.
    pub fn recv_with_auth(
        &mut self,
        peer_auth: &std::sync::Arc<std::sync::Mutex<crate::peer_auth::PeerAllowList>>,
    ) {
        let envelopes = self.acceptor.try_recv(&mut self.streams);
        for (envelope, _peer_addr, hints_bytes) in envelopes {
            // Extract sender NodeId from hints
            let mut sender_node_id = None;
            if !hints_bytes.is_empty()
                && let Ok(hints) = serde_json::from_slice::<Vec<AddressHint>>(&hints_bytes) {
                    if let Some(first) = hints.first() {
                        sender_node_id = Some(first.node_id);
                    }
                    self.learn_hints(&hints);
                }

            // Check peer auth if we know the sender
            if let Some(node_id) = sender_node_id {
                let allowed = peer_auth.lock().unwrap().is_allowed(&node_id);
                if !allowed {
                    let hex: String = node_id.0[..4].iter().map(|b| format!("{b:02x}")).collect();
                    eprintln!("driver: rejected message from unauthorized peer {hex}");
                    continue;
                }
            }

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
        // Standard sender hint
        let sender_hint = AddressHint {
            node_id: self.node.node_id(),
            addr: self.listen_addr,
        };

        match action {
            NodeAction::SendPing {
                to,
                sequence,
                piggyback,
            } => {
                let dest = self.resolve_addr(to)?;
                let msg = Ping {
                    from: self.node.node_id(),
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_wire_with_hints::<Ping>(&msg, dest, &[sender_hint])
            }

            NodeAction::SendAck {
                to,
                sequence,
                piggyback,
            } => {
                let dest = self.resolve_addr(to)?;
                let msg = Ack {
                    from: self.node.node_id(),
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_wire_with_hints::<Ack>(&msg, dest, &[sender_hint])
            }

            NodeAction::SendPingReq {
                relay,
                target,
                sequence,
                piggyback,
            } => {
                let dest = self.resolve_addr(relay)?;
                let msg = PingReq {
                    from: self.node.node_id(),
                    target: *target,
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                // Include target hint so the relay can forward
                let mut hints = vec![sender_hint];
                if let Some(target_addr) = self.address_book.resolve(target) {
                    hints.push(AddressHint {
                        node_id: *target,
                        addr: target_addr,
                    });
                }
                self.send_wire_with_hints::<PingReq>(&msg, dest, &hints)
            }

            NodeAction::SendJoinResponse {
                to, members,
            } => {
                let dest = self.resolve_addr(to)?;
                let msg = JoinResponse {
                    members: members.clone(),
                };
                // Include all known member addresses as hints
                let mut hints = vec![sender_hint];
                for record in members {
                    if let Some(addr) = self.address_book.resolve(&record.node_id) {
                        hints.push(AddressHint {
                            node_id: record.node_id,
                            addr,
                        });
                    }
                }
                self.send_wire_with_hints::<JoinResponse>(&msg, dest, &hints)
            }

            NodeAction::ForwardAck {
                to,
                target,
                sequence,
                piggyback,
            } => {
                let dest = self.resolve_addr(to)?;
                let msg = IndirectAck {
                    target: *target,
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_wire_with_hints::<IndirectAck>(&msg, dest, &[sender_hint])
            }

            NodeAction::MembershipChanged { .. } => {
                // Internal notification — no network I/O.
                Ok(())
            }
        }
    }

    fn resolve_addr(&self, node_id: &NodeId) -> Result<SocketAddr, swactor::Error> {
        self.address_book
            .resolve(node_id)
            .ok_or_else(|| swactor::Error::from(format!(
                "no address known for node {:?}",
                node_id
            )))
    }

    fn send_wire_with_hints<M: NetworkMessage + serde::Serialize>(
        &mut self,
        msg: &M,
        dest_addr: SocketAddr,
        hints: &[AddressHint],
    ) -> Result<(), swactor::Error> {
        let payload = serde_json::to_vec(msg)
            .map_err(|e| swactor::Error::from(format!("encode {}: {e}", M::type_tag())))?;
        let hints_bytes = serde_json::to_vec(hints).unwrap_or_default();
        let envelope = WireEnvelope {
            dest: SWIM_DEST,
            type_tag: M::type_tag().to_string(),
            payload,
        };
        let buf = encode_wire_envelope_with_hints(&envelope, &hints_bytes);
        let mut stream = self.transport_get_or_connect(dest_addr)?;
        use std::io::Write;
        match stream.write_all(&buf) {
            Ok(()) => Ok(()),
            Err(_) => {
                self.transport.evict(dest_addr);
                let mut stream = self.transport_get_or_connect(dest_addr)?;
                stream
                    .write_all(&buf)
                    .map_err(|e| swactor::Error::from(format!("TCP send to {dest_addr}: {e}")))
            }
        }
    }

    fn transport_get_or_connect(&self, addr: SocketAddr) -> Result<TcpStream, swactor::Error> {
        self.transport.get_or_connect(addr)
    }

    // ─── Incoming: TCP → handler ────────────────────────────────────────

    fn dispatch_incoming(&mut self, envelope: WireEnvelope) -> Vec<NodeAction> {
        match envelope.type_tag.as_str() {
            "swactor_dist::Ping" => match decode::<Ping>(&envelope.payload) {
                Ok(msg) => {
                    self.node.handle_ping(
                        msg.from,
                        msg.sequence,
                        &msg.piggyback,
                    )
                }
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
                    msg.sequence,
                    &msg.piggyback,
                ),
                Err(e) => {
                    eprintln!("driver: decode PingReq: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::JoinRequest" => match decode::<JoinRequest>(&envelope.payload) {
                Ok(msg) => self.node.handle_join_request(msg.from),
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

            "swactor_dist::IndirectAck" => match decode::<IndirectAck>(&envelope.payload) {
                Ok(msg) => self.node.handle_indirect_ack(msg.target, msg.sequence, &msg.piggyback),
                Err(e) => {
                    eprintln!("driver: decode IndirectAck: {e}");
                    Vec::new()
                }
            },

            other => {
                eprintln!("driver: unknown message type: {other}");
                Vec::new()
            }
        }
    }

    /// Process address hints extracted from incoming frames.
    pub fn learn_hints(&mut self, hints: &[AddressHint]) {
        self.address_book.bulk_learn(hints);
    }
}

fn decode<M: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<M, String> {
    serde_json::from_slice(bytes).map_err(|e| e.to_string())
}

fn parse_node_id_hex(hex: &str) -> Option<NodeId> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(NodeId(bytes))
}

