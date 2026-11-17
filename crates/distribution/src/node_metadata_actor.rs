//! `MetadataActor` — per-node metadata dissemination (`NodeMetadataDisseminator`)
//! as an actor.
//!
//! Node metadata travels in standalone
//! [`MetadataGossip`](crate::messages::MetadataGossip) frames. The default mode
//! uses timed dissemination budgets; [`MetadataActor::with_event_dissemination`]
//! broadcasts winning mutations immediately and sends snapshots on peer
//! establishment. Membership is supplied through [`MetadataIn::Membership`].
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
use crate::node_metadata::{NodeMetadataDisseminator, NodeMetadataEntry};
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
    /// Local: send all current metadata over a newly established connection,
    /// including replacements that do not cause a membership transition.
    SyncTo { peer: NodeId },
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
    event_dissemination: bool,
    /// Event mode retains generations after death to reject stale gossip, while
    /// membership hides those nodes from relay lookup and the published mirror.
    dead: BTreeSet<NodeId>,
    /// Current local relay/name, retained so setting one preserves the other
    /// (the engine's `set_local` rewrites the whole entry).
    self_relay: Option<String>,
    self_name: Option<String>,
    /// Explicit local settings remain authoritative over restored self gossip.
    /// A configured `None` relay is distinct from an unconfigured relay.
    self_relay_authored: bool,
    self_name_authored: bool,
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
            event_dissemination: false,
            dead: BTreeSet::new(),
            self_relay: None,
            self_name: None,
            self_relay_authored: false,
            self_name_authored: false,
        }
    }

    /// Broadcast every winning mutation to all alive peers without clock input.
    /// Alive notifications and `SyncTo` send full snapshots. Dead-node metadata
    /// remains versioned but is hidden until Alive; an explicit `None` relay URL
    /// is a versioned removal and propagates just like every other mutation.
    /// Explicit local settings supersede conflicting restored self metadata;
    /// unconfigured companion fields may be recovered from that metadata.
    pub fn with_event_dissemination(mut self) -> Self {
        self.event_dissemination = true;
        self.metadata.enable_event_dissemination();
        self
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
            if !self.dead.contains(&node)
                && let Some(url) = self.metadata.relay_url(&node)
            {
                mirror.insert(node, url.to_string());
            }
        }
    }

    /// Recover unconfigured fields from the highest self version received and
    /// request one reassertion when that version conflicts with local settings.
    /// Equal-generation conflicts matter after a restart; older conflicts do not.
    fn restore_self_metadata(&mut self, entries: &[NodeMetadataEntry]) -> bool {
        let current_generation = self.metadata.peer_version(&self.self_id).unwrap_or(0);
        let conflicts = |entry: &NodeMetadataEntry| {
            (self.self_relay_authored && entry.relay_url != self.self_relay)
                || (self.self_name_authored && entry.node_name != self.self_name)
        };
        let Some(restored) = entries
            .iter()
            .filter(|entry| entry.node_id == self.self_id && entry.generation >= current_generation)
            // Prefer a conflicting entry at the same watermark so a matching
            // echo in the same batch cannot conceal an equal-version conflict.
            .max_by_key(|entry| (entry.generation, conflicts(entry)))
        else {
            return false;
        };
        if !conflicts(restored) {
            return false;
        }
        if !self.self_relay_authored {
            self.self_relay.clone_from(&restored.relay_url);
        }
        if !self.self_name_authored {
            self.self_name.clone_from(&restored.node_name);
        }
        true
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

    fn disseminate_mutations(&mut self, ctx: &Ctx) {
        if !self.event_dissemination {
            return;
        }
        let mut entries = self.metadata.drain_pending();
        loop {
            let batch: Vec<_> = entries.by_ref().take(4).collect();
            if batch.is_empty() {
                break;
            }
            for peer in &self.alive {
                if let Some(addr) = self.peer_directory.resolve(peer) {
                    let _ = ctx.send(
                        addr,
                        MetadataIn::Gossip(MetadataGossip {
                            entries: batch.clone(),
                        }),
                    );
                }
            }
        }
    }

    fn sync_to(&self, ctx: &Ctx, peer: NodeId) {
        let Some(addr) = self.peer_directory.resolve(&peer) else {
            return;
        };
        let mut entries = self.metadata.entries();
        loop {
            let batch: Vec<_> = entries.by_ref().take(4).cloned().collect();
            if batch.is_empty() {
                break;
            }
            let _ = ctx.send(addr, MetadataIn::Gossip(MetadataGossip { entries: batch }));
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
                    if m.node_id != self.self_id {
                        let joined = self.alive.insert(m.node_id);
                        if self.event_dissemination {
                            self.dead.remove(&m.node_id);
                            self.refresh_relay_mirror();
                            self.sync_to(ctx, m.node_id);
                        } else if joined {
                            let size = self.cluster_size();
                            self.metadata.re_disseminate_all(size);
                        }
                    }
                }
                MemberState::Dead => {
                    self.alive.remove(&m.node_id);
                    if self.event_dissemination {
                        self.dead.insert(m.node_id);
                    } else {
                        self.metadata.remove_node(&m.node_id);
                    }
                    self.refresh_relay_mirror();
                }
                MemberState::Suspect => {}
            },
            MetadataIn::SetRelayUrl { url } => {
                self.self_relay = url;
                self.self_relay_authored = true;
                if self.event_dissemination && !self.self_name_authored {
                    self.self_name = self.metadata.node_name(&self.self_id).map(String::from);
                }
                let size = self.cluster_size();
                self.metadata.set_local(
                    self.self_id,
                    self.self_relay.clone(),
                    self.self_name.clone(),
                    size,
                );
                self.refresh_relay_mirror();
                self.disseminate_mutations(ctx);
            }
            MetadataIn::SetNodeName { name } => {
                self.self_name = Some(name);
                self.self_name_authored = true;
                if self.event_dissemination && !self.self_relay_authored {
                    self.self_relay = self.metadata.relay_url(&self.self_id).map(String::from);
                }
                let size = self.cluster_size();
                self.metadata.set_local(
                    self.self_id,
                    self.self_relay.clone(),
                    self.self_name.clone(),
                    size,
                );
                if self.event_dissemination {
                    self.refresh_relay_mirror();
                    self.disseminate_mutations(ctx);
                }
            }
            MetadataIn::RelayLookup { node, reply } => {
                let relay_url = if self.dead.contains(&node) {
                    None
                } else {
                    self.metadata.relay_url(&node).map(String::from)
                };
                let _ = ctx.send(reply, RelayInfo { node, relay_url });
            }
            MetadataIn::Gossip(g) => {
                let size = self.cluster_size();
                let reassert = self.event_dissemination && self.restore_self_metadata(&g.entries);
                self.metadata.apply_incoming(g.entries, size);
                if reassert {
                    self.metadata.set_local(
                        self.self_id,
                        self.self_relay.clone(),
                        self.self_name.clone(),
                        size,
                    );
                }
                self.refresh_relay_mirror();
                self.disseminate_mutations(ctx);
            }
            MetadataIn::SyncTo { peer } => self.sync_to(ctx, peer),
            MetadataIn::Tick if !self.event_dissemination => {
                self.disseminate(ctx);
            }
            MetadataIn::Tick => {}
        }
    }
}
