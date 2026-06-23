//! `MetadataActor` — per-node metadata dissemination (`NodeMetadataDisseminator`)
//! as an actor.
//!
//! Wraps the unchanged disseminator engine. Like the registry, node metadata
//! (relay URL + human name) used to ride the SWIM piggyback; it now travels as a
//! standalone [`MetadataGossip`](crate::messages::MetadataGossip) frame on its own
//! `Tick` cadence, and learns membership from the SwimActor's `MembershipChanged`
//! stream (adapted into [`MetadataIn::Membership`]).
//!
//! The network egress needs a peer's relay URL synchronously when it dials (it
//! can't block to `ask` an actor), so the actor mirrors every known
//! `NodeId → relay_url` into a shared [`RelayMirror`](crate::transport_bridge::RelayMirror)
//! the driver reads lock-free-ish on the dial path.

use std::collections::BTreeSet;
use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;

use crate::messages::MetadataGossip;
use crate::node_metadata::NodeMetadataDisseminator;
use crate::swim::actor::{MembershipChanged, PeerDirectory};
use crate::transport_bridge::RelayMirror;
use crate::types::{MemberState, NodeId};

/// Everything the `MetadataActor` receives (only `Gossip` crosses the wire).
#[derive(Clone)]
pub enum MetadataIn {
    /// Membership delta, adapted from the SwimActor's `MembershipChanged` stream.
    Membership(MembershipChanged),
    /// Local: set this node's relay URL and begin gossiping it.
    SetRelayUrl { url: Option<String> },
    /// Local: set this node's human-readable name and begin gossiping it.
    SetNodeName { name: String },
    /// Local request: look up `node`'s relay URL; result sent to `reply`.
    RelayLookup { node: NodeId, reply: ActorAddress },
    /// Gossip from a peer: a batch of metadata entries to merge.
    Gossip(MetadataGossip),
    /// Clock: disseminate a pending batch to one peer.
    Tick,
}

/// Reply to [`MetadataIn::RelayLookup`].
#[derive(Clone, Debug)]
pub struct RelayInfo {
    pub node: NodeId,
    pub relay_url: Option<String>,
}

pub struct MetadataActor {
    self_id: NodeId,
    metadata: NodeMetadataDisseminator,
    peer_directory: Arc<dyn PeerDirectory>,
    relay_mirror: RelayMirror,
    alive: BTreeSet<NodeId>,
    fanout_cursor: usize,
    /// Current local relay/name, retained so setting one preserves the other
    /// (the engine's `set_local` rewrites the whole entry).
    self_relay: Option<String>,
    self_name: Option<String>,
}

impl MetadataActor {
    pub fn new(
        self_id: NodeId,
        lambda: usize,
        peer_directory: Arc<dyn PeerDirectory>,
        relay_mirror: RelayMirror,
    ) -> Self {
        Self {
            self_id,
            metadata: NodeMetadataDisseminator::new(lambda),
            peer_directory,
            relay_mirror,
            alive: BTreeSet::new(),
            fanout_cursor: 0,
            self_relay: None,
            self_name: None,
        }
    }

    fn cluster_size(&self) -> usize {
        self.alive.len() + 1
    }

    /// Republish the relay read-mirror from the engine's current view, so the
    /// driver's dial path always sees the freshest `NodeId → relay_url`.
    fn refresh_relay_mirror(&self) {
        let mut mirror = self.relay_mirror.write().expect("relay mirror poisoned");
        mirror.clear();
        for (node, _gen) in self.metadata.all_versions() {
            if let Some(url) = self.metadata.relay_url(&node) {
                mirror.insert(node, url.to_string());
            }
        }
    }

    fn disseminate(&mut self, ctx: &Ctx) {
        if self.alive.is_empty() {
            return;
        }
        let entries = self.metadata.take_pending(4);
        if entries.is_empty() {
            return;
        }
        let peers: Vec<NodeId> = self.alive.iter().copied().collect();
        let peer = peers[self.fanout_cursor % peers.len()];
        self.fanout_cursor = self.fanout_cursor.wrapping_add(1);
        if let Some(addr) = self.peer_directory.resolve(&peer) {
            let _ = ctx.send(addr, MetadataIn::Gossip(MetadataGossip { entries }));
        }
    }
}

impl ActorInterface for MetadataActor {
    type Incoming = MetadataIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: MetadataIn) {
        match msg {
            MetadataIn::Membership(m) => match m.state {
                MemberState::Alive => {
                    if m.node_id != self.self_id && self.alive.insert(m.node_id) {
                        let size = self.cluster_size();
                        self.metadata.re_disseminate_all(size);
                    }
                }
                MemberState::Dead => {
                    self.alive.remove(&m.node_id);
                    self.metadata.remove_node(&m.node_id);
                    self.refresh_relay_mirror();
                }
                MemberState::Suspect => {}
            },
            MetadataIn::SetRelayUrl { url } => {
                self.self_relay = url;
                let size = self.cluster_size();
                self.metadata.set_local(
                    self.self_id,
                    self.self_relay.clone(),
                    self.self_name.clone(),
                    size,
                );
                self.refresh_relay_mirror();
            }
            MetadataIn::SetNodeName { name } => {
                self.self_name = Some(name);
                let size = self.cluster_size();
                self.metadata.set_local(
                    self.self_id,
                    self.self_relay.clone(),
                    self.self_name.clone(),
                    size,
                );
            }
            MetadataIn::RelayLookup { node, reply } => {
                let relay_url = self.metadata.relay_url(&node).map(String::from);
                let _ = ctx.send(reply, RelayInfo { node, relay_url });
            }
            MetadataIn::Gossip(g) => {
                let size = self.cluster_size();
                self.metadata.apply_incoming(g.entries, size);
                self.refresh_relay_mirror();
            }
            MetadataIn::Tick => {
                self.disseminate(ctx);
            }
        }
    }
}
