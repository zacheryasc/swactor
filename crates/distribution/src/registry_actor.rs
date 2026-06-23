//! `RegistryActor` — the cluster name registry (`ClusterRegistry`) as an actor.
//!
//! Wraps the unchanged `ClusterRegistry` CRDT engine. Unlike the pre-actor design
//! (where registry entries were packed into the SWIM piggyback), the registry now
//! owns a standalone gossip frame ([`RegistryGossip`](crate::messages::RegistryGossip))
//! and its own dissemination cadence, driven by `Tick`. SWIM is membership-only;
//! the registry learns membership by subscribing to the SwimActor's
//! `MembershipChanged` stream (adapted into [`RegistryIn::Membership`] by a small
//! fanout actor), which drives tombstoning of dead nodes and anti-entropy when a
//! node returns.

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;

use crate::messages::RegistryGossip;
use crate::registry::{ClusterRegistry, RegistryConfig, RegistrySnapshot};
use crate::swim::actor::{MembershipChanged, PeerDirectory};
use crate::types::{MemberState, NodeId};

/// A single-writer read-mirror of the registry's observable state, published by
/// the [`RegistryActor`] after each change and read by the node's telemetry tick
/// (the same discipline as the directory's
/// [`RouteView`](crate::transport_bridge::RouteView)). Installed via
/// [`RegistryActor::with_view`]; absent in tests/examples that don't observe it.
pub type RegistryView = Arc<RwLock<RegistrySnapshot>>;

/// Everything the `RegistryActor` receives, as one enum (only `Gossip` crosses
/// the wire; the rest are local control — see [`crate::messages::actor_codec_registry`]).
#[derive(Clone)]
pub enum RegistryIn {
    /// Membership delta, adapted from the SwimActor's `MembershipChanged` stream.
    Membership(MembershipChanged),
    /// Local: register a name → actor binding owned by this node.
    RegisterName {
        name: String,
        actor_addr: ActorAddress,
    },
    /// Local: unregister a name (writes a tombstone).
    UnregisterName { name: String },
    /// Local request: resolve `name`; the result is sent to `reply`.
    ResolveName { name: String, reply: ActorAddress },
    /// Gossip from a peer: a batch of CRDT entries to merge.
    Gossip(RegistryGossip),
    /// Clock: run GC and disseminate a pending batch to one peer.
    Tick,
}

/// Reply to [`RegistryIn::ResolveName`].
#[derive(Clone, Debug)]
pub struct NameResolved {
    pub name: String,
    pub binding: Option<(ActorAddress, NodeId)>,
}

pub struct RegistryActor {
    self_id: NodeId,
    registry: ClusterRegistry,
    peer_directory: Arc<dyn PeerDirectory>,
    /// Alive peers (excludes self), folded from the membership stream — the basis
    /// for `cluster_size` (dissemination budget) and the gossip fan-out set.
    alive: BTreeSet<NodeId>,
    /// Round-robins the gossip target across alive peers, one per `Tick` — the
    /// standalone analog of "piggyback on the next probe".
    fanout_cursor: usize,
    /// Optional read-mirror the node's telemetry tick observes. Republished
    /// after each registry change. `None` when no one is observing.
    view: Option<RegistryView>,
}

impl RegistryActor {
    pub fn new(
        self_id: NodeId,
        config: RegistryConfig,
        peer_directory: Arc<dyn PeerDirectory>,
    ) -> Self {
        Self {
            self_id,
            registry: ClusterRegistry::new(config),
            peer_directory,
            alive: BTreeSet::new(),
            fanout_cursor: 0,
            view: None,
        }
    }

    /// Install a read-mirror that this actor republishes after each change, so a
    /// telemetry consumer can read live registry figures without `ask`-ing it.
    /// Seeds the mirror with the current (empty) snapshot immediately.
    pub fn with_view(mut self, view: RegistryView) -> Self {
        *view.write().expect("registry view poisoned") = self.registry.snapshot();
        self.view = Some(view);
        self
    }

    /// Republish the registry snapshot to the read-mirror, if one is installed.
    fn publish(&self) {
        if let Some(view) = &self.view {
            *view.write().expect("registry view poisoned") = self.registry.snapshot();
        }
    }

    fn cluster_size(&self) -> usize {
        self.alive.len() + 1 // +1 for self
    }

    /// Send the next pending dissemination batch to one alive peer (round-robin),
    /// mirroring how entries used to ride the next SWIM probe. Skips taking from
    /// the queue when there is no peer to receive it, so budget isn't burned into
    /// the void.
    fn disseminate(&mut self, ctx: &Ctx) {
        if self.alive.is_empty() {
            return;
        }
        let entries = self.registry.take_pending(8);
        if entries.is_empty() {
            return;
        }
        let peers: Vec<NodeId> = self.alive.iter().copied().collect();
        let peer = peers[self.fanout_cursor % peers.len()];
        self.fanout_cursor = self.fanout_cursor.wrapping_add(1);
        if let Some(addr) = self.peer_directory.resolve(&peer) {
            let _ = ctx.send(addr, RegistryIn::Gossip(RegistryGossip { entries }));
        }
    }
}

impl ActorInterface for RegistryActor {
    type Incoming = RegistryIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: RegistryIn) {
        match msg {
            RegistryIn::Membership(m) => match m.state {
                MemberState::Alive => {
                    if m.node_id != self.self_id && self.alive.insert(m.node_id) {
                        // A (re)joining peer: re-gossip everything so it catches up
                        // on state accumulated while it was away.
                        let size = self.cluster_size();
                        self.registry.re_disseminate_all(size);
                    }
                }
                MemberState::Dead => {
                    self.alive.remove(&m.node_id);
                    let size = self.cluster_size();
                    self.registry.tombstone_node(m.node_id, size);
                    self.publish();
                }
                MemberState::Suspect => {}
            },
            RegistryIn::RegisterName { name, actor_addr } => {
                let size = self.cluster_size();
                self.registry.register(name, actor_addr, self.self_id, size);
                self.publish();
            }
            RegistryIn::UnregisterName { name } => {
                let size = self.cluster_size();
                self.registry.unregister(&name, self.self_id, size);
                self.publish();
            }
            RegistryIn::ResolveName { name, reply } => {
                let binding = self.registry.resolve(&name);
                let _ = ctx.send(reply, NameResolved { name, binding });
            }
            RegistryIn::Gossip(g) => {
                let size = self.cluster_size();
                self.registry.merge_batch(g.entries, size);
                self.publish();
            }
            RegistryIn::Tick => {
                self.registry.gc_tick();
                self.disseminate(ctx);
                // GC may have reaped tombstones; keep the mirror current.
                self.publish();
            }
        }
    }
}
