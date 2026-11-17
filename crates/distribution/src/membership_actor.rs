//! Membership driven by authenticated cluster connections, not failure-detector clocks.
//!
//! Roster messages discover peers; only a transport observation admits a peer.
//! Connection closure and explicit leave remove it. Newly discovered peers receive
//! one join request, and the transport owns the resulting connection attempt.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;

use crate::messages::{JoinRequest, JoinResponse, MembershipGossip, MembershipPeer};
use crate::swim::actor::{MembershipChanged, PeerDirectory, SwimIn};
use crate::types::{MemberState, NodeId, NodeRecord};

#[derive(Clone, Debug)]
pub struct ObservedMembershipTransition {
    pub peer: NodeId,
    pub from: Option<MemberState>,
    pub to: MemberState,
    pub reason: &'static str,
    pub incarnation: u64,
}

pub struct TransportMembershipActor {
    self_id: NodeId,
    peers: Arc<dyn PeerDirectory>,
    connected: BTreeMap<NodeId, MembershipPeer>,
    local: Option<MembershipPeer>,
    pending: BTreeSet<NodeId>,
    states: BTreeMap<NodeId, (MemberState, u64)>,
    observer: ActorAddress,
    subscribers: Vec<ActorAddress>,
    leaving: bool,
}

impl TransportMembershipActor {
    pub fn new(self_id: NodeId, peers: Arc<dyn PeerDirectory>, observer: ActorAddress) -> Self {
        Self {
            self_id,
            peers,
            connected: BTreeMap::new(),
            local: None,
            pending: BTreeSet::new(),
            states: BTreeMap::new(),
            observer,
            subscribers: Vec::new(),
            leaving: false,
        }
    }

    fn send(&self, ctx: &Ctx<'_>, peer: NodeId, message: SwimIn) {
        if let Some(address) = self.peers.resolve(&peer) {
            let _ = ctx.send(address, message);
        }
    }

    fn observe(&mut self, ctx: &Ctx<'_>, peer: NodeId, state: MemberState, reason: &'static str) {
        let previous = self.states.get(&peer).copied();
        if previous.is_some_and(|(current, _)| current == state) {
            return;
        }
        let incarnation = previous.map_or(1, |(_, revision)| revision.saturating_add(1));
        self.states.insert(peer, (state, incarnation));
        let _ = ctx.send(
            self.observer,
            ObservedMembershipTransition {
                peer,
                from: previous.map(|(state, _)| state),
                to: state,
                reason,
                incarnation,
            },
        );
        for subscriber in &self.subscribers {
            let _ = ctx.send(
                *subscriber,
                MembershipChanged {
                    node_id: peer,
                    state,
                    incarnation,
                },
            );
        }
    }

    fn discover(&mut self, ctx: &Ctx<'_>, peer: NodeId) {
        if peer != self.self_id
            && !self.leaving
            && !self.connected.contains_key(&peer)
            && self.pending.insert(peer)
        {
            self.send(
                ctx,
                peer,
                SwimIn::JoinRequest(JoinRequest { from: self.self_id }),
            );
        }
    }

    fn gossip(&self, ctx: &Ctx<'_>) {
        let Some(local) = &self.local else {
            return;
        };
        let message = SwimIn::MembershipGossip(MembershipGossip {
            from: local.clone(),
            peers: self.connected.values().cloned().collect(),
            leaving: self.leaving,
        });
        for peer in self.connected.keys() {
            self.send(ctx, *peer, message.clone());
        }
    }
}

impl ActorInterface for TransportMembershipActor {
    type Incoming = SwimIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, message: SwimIn) {
        match message {
            SwimIn::TransportConnected { peer, local } => {
                self.local = Some(local);
                let node_id = peer.node_id;
                self.pending.remove(&node_id);
                if !self.leaving && node_id != self.self_id {
                    let previous = self.connected.insert(node_id, peer);
                    if previous.is_none() {
                        self.observe(ctx, node_id, MemberState::Alive, "transport_connected");
                    }
                    self.gossip(ctx);
                }
            }
            SwimIn::TransportDisconnected { peer } => {
                self.pending.remove(&peer);
                if self.connected.remove(&peer).is_some() {
                    self.observe(ctx, peer, MemberState::Dead, "transport_closed");
                    self.gossip(ctx);
                }
            }
            SwimIn::SendFailed { to } => {
                // A later discovery may retry admission; send failure is not disconnection.
                self.pending.remove(&to);
            }
            SwimIn::JoinRequest(request) => {
                if self.leaving {
                    return;
                }
                let members = std::iter::once(self.self_id)
                    .chain(self.connected.keys().copied())
                    .map(|node_id| NodeRecord {
                        node_id,
                        state: MemberState::Alive,
                        incarnation: self
                            .states
                            .get(&node_id)
                            .map_or(1, |(_, revision)| *revision),
                    })
                    .collect();
                self.send(
                    ctx,
                    request.from,
                    SwimIn::JoinResponse(JoinResponse { members }),
                );
            }
            SwimIn::JoinResponse(response) => {
                for member in response.members {
                    if member.state == MemberState::Alive {
                        self.discover(ctx, member.node_id);
                    }
                }
            }
            SwimIn::MembershipGossip(gossip) => {
                let from = gossip.from.node_id;
                if gossip.leaving {
                    self.pending.remove(&from);
                    if self.connected.remove(&from).is_some() {
                        self.observe(ctx, from, MemberState::Dead, "peer_left");
                        self.gossip(ctx);
                    }
                } else {
                    if let Some(known) = self.connected.get_mut(&from) {
                        let mut advertised = gossip.from;
                        if advertised.direct_addrs.is_empty() {
                            advertised.direct_addrs.clone_from(&known.direct_addrs);
                        }
                        if advertised.relay_url.is_none() {
                            advertised.relay_url.clone_from(&known.relay_url);
                        }
                        if *known != advertised {
                            *known = advertised;
                            self.gossip(ctx);
                        }
                    }
                    for peer in gossip.peers {
                        self.discover(ctx, peer.node_id);
                    }
                }
            }
            SwimIn::Join { seeds } => {
                for peer in seeds {
                    self.discover(ctx, peer);
                }
            }
            SwimIn::Leave => {
                if !self.leaving {
                    self.leaving = true;
                    self.gossip(ctx);
                    self.pending.clear();
                }
            }
            SwimIn::Subscribe { observer } => {
                for (&node_id, &(state, incarnation)) in &self.states {
                    let _ = ctx.send(
                        observer,
                        MembershipChanged {
                            node_id,
                            state,
                            incarnation,
                        },
                    );
                }
                self.subscribers.push(observer);
            }
            // Probe clocks and probe failures do not define transport membership.
            SwimIn::Tick { .. }
            | SwimIn::Ping(_)
            | SwimIn::Ack(_)
            | SwimIn::PingReq(_)
            | SwimIn::IndirectAck(_) => {}
        }
    }
}
