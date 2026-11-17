//! `RegistryActor` — the cluster name registry (`ClusterRegistry`) as an actor.
//!
//! Wraps the `ClusterRegistry` CRDT engine in a standalone
//! [`RegistryGossip`](crate::messages::RegistryGossip) protocol. By default,
//! dissemination and tombstone GC are driven by `Tick`. The opt-in
//! [`RegistryActor::with_event_dissemination`] mode broadcasts winning mutations
//! immediately and retains tombstones for late peers. Membership deltas drive
//! dead-node tombstones and full snapshots on peer establishment.

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock, Weak};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;

use crate::messages::{RegistryDelivery, RegistryGossip};
use crate::registry::{ClusterRegistry, RegistryConfig, RegistryEntry, RegistrySnapshot};
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
    /// Local: register with a caller-supplied logical timestamp. Used by a
    /// recovered service whose fresh logical clock would otherwise lose to the
    /// binding it disseminated before restarting.
    RegisterNameAt {
        name: String,
        actor_addr: ActorAddress,
        timestamp: u64,
    },
    /// Local: push this registry's current live entry for `name` directly to
    /// `peers` now, bypassing SWIM-gated dissemination. Recovery re-bind: a
    /// restarted authority must replace the dead binding workers still serve
    /// without waiting for membership rounds to re-form (observed stall: a
    /// dead directory binding served for the full namespace request deadline
    /// while membership re-converged).
    DisseminateNameTo {
        name: String,
        peers: Vec<NodeId>,
        reply_to: ActorAddress,
    },
    /// Local: a peer installed this exact live name/binding/generation.
    /// Delivered only while it remains the origin registry's current winner.
    NameAcknowledged { entry: RegistryEntry, peer: NodeId },
    /// Local: unregister a name (writes a tombstone).
    UnregisterName { name: String },
    /// Local request: resolve `name`; the result is sent to `reply`.
    ResolveName { name: String, reply: ActorAddress },
    /// Gossip from a peer: a batch of CRDT entries to merge.
    Gossip(RegistryGossip),
    /// Local: send all current winners, including tombstones, directly over a
    /// newly established peer connection, even if membership never changed.
    SyncTo { peer: NodeId },
    /// Local observer, retained only while its owner keeps the callback alive.
    /// Runs after publication and once on registration to close the check/subscribe race.
    WatchName {
        name: String,
        changed: Weak<dyn Fn() + Send + Sync>,
    },
    /// Clock: run GC and disseminate a pending batch to one peer.
    Tick,
}

/// Reply to [`RegistryIn::ResolveName`].
#[derive(Clone, Debug)]
pub struct NameResolved {
    pub name: String,
    pub binding: Option<(ActorAddress, NodeId)>,
}

type RegistryWatcher = (String, Option<RegistryEntry>, Weak<dyn Fn() + Send + Sync>);

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
    watchers: Vec<RegistryWatcher>,
    event_dissemination: bool,
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
            watchers: Vec::new(),
            event_dissemination: false,
        }
    }

    /// Broadcast every winning mutation to all alive peers without clock input.
    /// Alive notifications and `SyncTo` send full snapshots, including retained
    /// tombstones. `Tick` does not advance this mode or collect its tombstones:
    /// discarding them without causal acknowledgement would resurrect old names.
    pub fn with_event_dissemination(mut self) -> Self {
        self.event_dissemination = true;
        self.registry.enable_event_dissemination();
        self
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
    fn publish(&mut self) {
        if let Some(view) = &self.view {
            *view.write().expect("registry view poisoned") = self.registry.snapshot();
        }
        self.watchers.retain_mut(|(name, previous, changed)| {
            let Some(changed) = changed.upgrade() else {
                return false;
            };
            let current = self.registry.entries().find(|entry| entry.name == *name);
            if current != previous.as_ref() {
                *previous = current.cloned();
                changed();
            }
            true
        });
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
            let _ = ctx.send(
                addr,
                RegistryIn::Gossip(RegistryGossip {
                    entries,
                    delivery: None,
                }),
            );
        }
    }

    fn disseminate_mutations(&mut self, ctx: &Ctx) {
        if !self.event_dissemination {
            return;
        }
        // The engine enqueues only winning mutations, coalesced by name. Drain
        // once even with no peers: a later connection gets the complete state.
        let mut entries = self.registry.drain_pending();
        loop {
            let batch: Vec<_> = entries.by_ref().take(8).collect();
            if batch.is_empty() {
                break;
            }
            for peer in &self.alive {
                if let Some(addr) = self.peer_directory.resolve(peer) {
                    let _ = ctx.send(
                        addr,
                        RegistryIn::Gossip(RegistryGossip {
                            entries: batch.clone(),
                            delivery: None,
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
        let mut entries = self.registry.entries();
        loop {
            let batch: Vec<_> = entries.by_ref().take(8).cloned().collect();
            if batch.is_empty() {
                break;
            }
            let _ = ctx.send(
                addr,
                RegistryIn::Gossip(RegistryGossip {
                    entries: batch,
                    delivery: None,
                }),
            );
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
                    if m.node_id != self.self_id {
                        let joined = self.alive.insert(m.node_id);
                        if self.event_dissemination {
                            self.sync_to(ctx, m.node_id);
                        } else if joined {
                            let size = self.cluster_size();
                            self.registry.re_disseminate_all(size);
                        }
                    }
                }
                MemberState::Dead => {
                    self.alive.remove(&m.node_id);
                    let size = self.cluster_size();
                    self.registry.tombstone_node(m.node_id, size);
                    self.publish();
                    self.disseminate_mutations(ctx);
                }
                MemberState::Suspect => {}
            },
            RegistryIn::RegisterName { name, actor_addr } => {
                let size = self.cluster_size();
                self.registry.register(name, actor_addr, self.self_id, size);
                self.publish();
                self.disseminate_mutations(ctx);
            }
            RegistryIn::RegisterNameAt {
                name,
                actor_addr,
                timestamp,
            } => {
                let size = self.cluster_size();
                self.registry
                    .register_at(name, actor_addr, self.self_id, timestamp, size);
                self.publish();
                self.disseminate_mutations(ctx);
            }
            RegistryIn::DisseminateNameTo {
                name,
                peers,
                reply_to,
            } => {
                let Some(entry) = self
                    .registry
                    .entries()
                    .find(|entry| {
                        entry.name == name && !entry.tombstone && entry.node_id == self.self_id
                    })
                    .cloned()
                else {
                    return;
                };
                for peer in peers {
                    if let Some(addr) = self.peer_directory.resolve(&peer) {
                        let _ = ctx.send(
                            addr,
                            RegistryIn::Gossip(RegistryGossip {
                                entries: vec![entry.clone()],
                                delivery: Some(RegistryDelivery::Request { reply_to }),
                            }),
                        );
                    }
                }
            }
            RegistryIn::UnregisterName { name } => {
                let size = self.cluster_size();
                self.registry.unregister(&name, self.self_id, size);
                self.publish();
                self.disseminate_mutations(ctx);
            }
            RegistryIn::SyncTo { peer } => self.sync_to(ctx, peer),
            RegistryIn::WatchName { name, changed } => {
                let current = self
                    .registry
                    .entries()
                    .find(|entry| entry.name == name)
                    .cloned();
                if let Some(notify) = changed.upgrade() {
                    notify();
                    self.watchers.push((name, current, changed));
                }
            }
            RegistryIn::ResolveName { name, reply } => {
                let binding = self.registry.resolve(&name);
                let _ = ctx.send(reply, NameResolved { name, binding });
            }
            RegistryIn::Gossip(g) => {
                let size = self.cluster_size();
                match g.delivery {
                    Some(RegistryDelivery::Acknowledged { reply_to, peer }) => {
                        for entry in g.entries {
                            if !entry.tombstone
                                && entry.node_id == self.self_id
                                && self.registry.entries().any(|current| current == &entry)
                            {
                                let _ = ctx
                                    .send(reply_to, RegistryIn::NameAcknowledged { entry, peer });
                            }
                        }
                    }
                    Some(RegistryDelivery::Request { reply_to }) => {
                        self.registry.merge_batch(g.entries.iter().cloned(), size);
                        self.publish();
                        self.disseminate_mutations(ctx);
                        for entry in g.entries {
                            if !entry.tombstone
                                && self.registry.entries().any(|current| current == &entry)
                                && let Some(addr) = self.peer_directory.resolve(&entry.node_id)
                            {
                                let _ = ctx.send(
                                    addr,
                                    RegistryIn::Gossip(RegistryGossip {
                                        entries: vec![entry],
                                        delivery: Some(RegistryDelivery::Acknowledged {
                                            reply_to,
                                            peer: self.self_id,
                                        }),
                                    }),
                                );
                            }
                        }
                    }
                    None => {
                        self.registry.merge_batch(g.entries, size);
                        self.publish();
                        self.disseminate_mutations(ctx);
                    }
                }
            }
            RegistryIn::NameAcknowledged { .. } => {}
            RegistryIn::Tick if !self.event_dissemination => {
                self.registry.gc_tick();
                self.disseminate(ctx);
                // GC may have reaped tombstones; keep the mirror current.
                self.publish();
            }
            RegistryIn::Tick => {}
        }
    }
}
