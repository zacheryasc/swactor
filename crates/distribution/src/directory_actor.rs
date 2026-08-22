//! `DirectoryActor` — the actor→host location directory (`DIRECTORY.md`) as a
//! standalone gossip actor.
//!
//! It is the fourth standalone gossip actor, a peer of [`SwimActor`], [`RegistryActor`],
//! and [`MetadataActor`] — not a layer above them. It owns one signed claim per
//! actor (a [`DirectoryEntry`], the spec's `Claim`), converges that location map
//! across the cluster by lazy round-robin gossip on `Tick`, and publishes a
//! [`RouteView`] read-mirror the egress consults to route an application message to
//! an actor it knows only by address (`DIRECTORY.md` §5).
//!
//! It differs from the registry/metadata template in only three ways:
//!  1. it stores signed claims directly (no wrapped CRDT engine), one per actor;
//!  2. it **verifies the signature on every merge**, so a forged claim is dropped;
//!  3. it publishes a [`RouteView`] (`ActorAddress → NodeId`) instead of a
//!     [`RelayMirror`](crate::transport_bridge::RelayMirror) (`NodeId → relay URL`).
//!
//! Routing is **blind, best-effort**: there are no acks, no retries, no awaited
//! replies. The single wire-crossing edge is [`DirectoryIn::Gossip`]; every other
//! variant is local control. Like SWIM, the actor reads no ambient clock — time
//! enters only as `Tick`.
//!
//! [`SwimActor`]: crate::swim::actor::SwimActor
//! [`RegistryActor`]: crate::registry_actor::RegistryActor
//! [`MetadataActor`]: crate::node_metadata_actor::MetadataActor

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;

use crate::crypto::verify_directory_entry;
use crate::messages::DirectoryGossip;
use crate::swim::actor::{MembershipChanged, PeerDirectory};
use crate::transport_bridge::{RouteBinder, RouteView};
use crate::types::{DirectoryEntry, MemberState, NodeId};

/// Round-robin gossip fan-out per `Tick` (the base dissemination width).
const FANOUT: u32 = 3;
/// Budget ceiling, in multiples of [`FANOUT`]: a claim is sent at most
/// `FANOUT * MAX_ROUNDS` times before it stops being re-pushed.
const MAX_ROUNDS: u32 = 8;
/// Claims shipped in a single gossip batch per `Tick`.
const BATCH: usize = 16;
/// Dead hosts are hidden from the published [`RouteView`], but their signed
/// claims stay cached so a false-dead peer can become routable again as soon as
/// SWIM reports it Alive. Claim deletion requires a future explicit tombstone or
/// owner-side lifecycle signal; raw `Tick` cadence is not a safe GC clock.
/// Everything the `DirectoryActor` receives, as one enum. Only [`Gossip`](DirectoryIn::Gossip)
/// crosses the wire (it carries the registered `DirectoryGossip` tag); the rest
/// are local control — see [`crate::messages::actor_codec_registry`].
#[derive(Clone)]
pub enum DirectoryIn {
    /// Local: a host-signed claim for an actor spawned on this node. The host is
    /// the claim's signer; merging it begins disseminating it.
    Register(DirectoryEntry),
    /// Gossip from a peer: a batch of signed claims to merge.
    Gossip(DirectoryGossip),
    /// Membership delta, adapted from the SwimActor's `MembershipChanged` stream.
    Membership(MembershipChanged),
    /// Local diagnostic read (`DIRECTORY.md` §5): resolve `actor`'s host from the
    /// converged map; the answer is sent to `reply`. Off the hot path — the
    /// load-bearing read is the [`RouteView`], read directly by the egress.
    Resolve {
        actor: ActorAddress,
        reply: ActorAddress,
    },
    /// Clock: disseminate one batch to one peer.
    Tick,
}

/// Reply to [`DirectoryIn::Resolve`] (diagnostics/tooling only).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Located {
    pub actor: ActorAddress,
    pub host: Option<NodeId>,
}

pub struct DirectoryActor {
    self_id: NodeId,
    /// The location map: one signed claim per actor. The only writer is [`Self::merge_one`].
    map: HashMap<ActorAddress, DirectoryEntry>,
    /// Alive peers (excludes self), folded from the membership stream — the
    /// dissemination fan-out set and the `cluster_size` budget basis.
    alive: BTreeSet<NodeId>,
    /// Actors still owing dissemination → remaining sends. A freshly authored or
    /// freshly-superseded claim is armed here; budget decays to zero in steady
    /// state, so a settled cluster goes quiet.
    hot: HashMap<ActorAddress, u32>,
    /// Round-robins the gossip target across `alive`, one per `Tick`.
    cursor: usize,
    peer_directory: Arc<dyn PeerDirectory>,
    /// The §5 read mirror. Single writer = this actor; republished on every change.
    route_view: RouteView,
    /// Registers a route so the runtime can deliver an app message addressed to a
    /// remotely-hosted actor (the §5 egress seam). Called from [`Self::republish`].
    route_binder: Arc<dyn RouteBinder>,
    /// Routes owned outside SWIM membership (for example an exec child actor
    /// runtime attached to this host). Directory republishing always preserves
    /// these entries.
    pinned_routes: RouteView,
}

impl DirectoryActor {
    pub fn new(
        self_id: NodeId,
        peer_directory: Arc<dyn PeerDirectory>,
        route_view: RouteView,
        route_binder: Arc<dyn RouteBinder>,
    ) -> Self {
        Self::with_pinned_routes(
            self_id,
            peer_directory,
            route_view,
            Arc::new(std::sync::RwLock::new(HashMap::new())),
            route_binder,
        )
    }

    pub fn with_pinned_routes(
        self_id: NodeId,
        peer_directory: Arc<dyn PeerDirectory>,
        route_view: RouteView,
        pinned_routes: RouteView,
        route_binder: Arc<dyn RouteBinder>,
    ) -> Self {
        Self {
            self_id,
            map: HashMap::new(),
            alive: BTreeSet::new(),
            hot: HashMap::new(),
            cursor: 0,
            peer_directory,
            route_view,
            route_binder,
            pinned_routes,
        }
    }

    /// Merge one claim under the supersession rule — the only writer of `map`.
    ///
    /// A claim wins iff it is strictly newer by `(generation, node_id)`: a higher
    /// generation supersedes; at equal generation the larger `node_id` is the
    /// deterministic tie-break (so every node converges on the same winner). A
    /// stale, equal, or forged claim is ignored. Merging the same claim twice is a
    /// no-op — it does not re-arm dissemination, which is what lets the cluster go
    /// quiet. Idempotent and commutative.
    fn merge_one(&mut self, claim: DirectoryEntry) {
        if !verify_directory_entry(&claim) {
            return; // not signed by the host it names — drop it
        }
        let supersedes = match self.map.get(&claim.actor_addr) {
            None => true,
            Some(cur) => {
                claim.generation > cur.generation
                    || (claim.generation == cur.generation && claim.node_id > cur.node_id)
            }
        };
        if supersedes {
            let budget = self.budget();
            self.hot.insert(claim.actor_addr, budget); // arm for dissemination
            self.map.insert(claim.actor_addr, claim);
        }
    }

    fn register(&mut self, claim: DirectoryEntry) {
        self.merge_one(claim);
        self.republish();
    }

    fn merge_batch(&mut self, claims: Vec<DirectoryEntry>) {
        for c in claims {
            self.merge_one(c);
        }
        self.republish();
    }

    fn on_membership(&mut self, change: MembershipChanged) {
        match change.state {
            MemberState::Alive if change.node_id != self.self_id => {
                if self.alive.insert(change.node_id) {
                    // A (re)joining peer: re-arm every held claim so the returning
                    // peer is caught up — without a full-cluster reflood (only the
                    // actors we hold, and only via the lazy push).
                    let budget = self.budget();
                    let actors: Vec<ActorAddress> = self.map.keys().copied().collect();
                    for actor in actors {
                        self.hot.insert(actor, budget);
                    }
                }
            }
            MemberState::Dead => {
                // Drop from the alive set; republish hides its actors from routing.
                // Keep the signed claim cached so a false-dead host can recover
                // without depending on wall-clock or pump-tick based GC.
                self.alive.remove(&change.node_id);
            }
            _ => {} // Suspect, or self: ignore (suspicion is SWIM's transient state)
        }
        self.republish();
    }

    fn tick(&mut self, ctx: &Ctx) {
        self.disseminate(ctx);
    }

    /// Send one batch of armed claims to one alive peer (round-robin). Skips the
    /// send when there is no peer or nothing armed, so budget is never burned into
    /// the void — mirrors the registry/metadata disseminators.
    fn disseminate(&mut self, ctx: &Ctx) {
        if self.alive.is_empty() || self.hot.is_empty() {
            return;
        }
        let peers: Vec<NodeId> = self.alive.iter().copied().collect();
        let peer = peers[self.cursor % peers.len()];
        self.cursor = self.cursor.wrapping_add(1);
        let claims = self.take_hot(BATCH);
        if claims.is_empty() {
            return;
        }
        if let Some(addr) = self.peer_directory.resolve(&peer) {
            let _ = ctx.send(addr, DirectoryIn::Gossip(DirectoryGossip { claims }));
        }
    }

    /// Republish the §5 route view: every actor whose host is reachable right now
    /// (self, or an alive peer). A dead host's actors are omitted, so the egress
    /// never routes to a host SWIM has buried; the claim remains cached for recovery.
    ///
    /// Builds the new view locally, swaps it in under the lock, then registers a
    /// route for each remotely-hosted actor *outside* the lock — so the egress
    /// (which takes the view's read lock on every send) never contends with the
    /// route registration.
    fn republish(&self) {
        let pinned = self
            .pinned_routes
            .read()
            .expect("pinned route view poisoned");
        let mut view = pinned.clone();
        let mut remote = pinned
            .iter()
            .filter_map(|(actor, node)| (*node != self.self_id).then_some(*actor))
            .collect::<Vec<_>>();
        drop(pinned);
        for (actor, claim) in &self.map {
            if view.contains_key(actor) {
                continue;
            }
            if claim.node_id == self.self_id {
                view.insert(*actor, claim.node_id);
            } else if self.alive.contains(&claim.node_id) {
                view.insert(*actor, claim.node_id);
                remote.push(*actor);
            }
        }
        *self.route_view.write().expect("route view poisoned") = view;
        for actor in remote {
            self.route_binder.ensure_routable(actor);
        }
    }

    /// Take up to `limit` armed claims for this tick's batch, spending one unit of
    /// each one's budget. A claim whose budget reaches zero stops being re-pushed.
    fn take_hot(&mut self, limit: usize) -> Vec<DirectoryEntry> {
        let actors: Vec<ActorAddress> = self.hot.keys().copied().take(limit).collect();
        let mut claims = Vec::with_capacity(actors.len());
        for actor in actors {
            if let Some(c) = self.map.get(&actor) {
                claims.push(c.clone());
            }
            if let Some(left) = self.hot.get_mut(&actor) {
                *left = left.saturating_sub(1);
                if *left == 0 {
                    self.hot.remove(&actor);
                }
            }
        }
        claims
    }

    /// Per-claim dissemination budget: `FANOUT · ⌈log2(cluster+1)⌉`, clamped to
    /// `[FANOUT, FANOUT·MAX_ROUNDS]`. Grows gently with cluster size so a claim
    /// reaches enough seeds for epidemic spread to finish the job, but is bounded.
    fn budget(&self) -> u32 {
        let n = (self.alive.len() + 1) as u32; // include self
        let log2 = u32::BITS - n.leading_zeros(); // bit-length of n ≈ ⌈log2(n+1)⌉
        (FANOUT * log2).clamp(FANOUT, FANOUT * MAX_ROUNDS)
    }
}

impl ActorInterface for DirectoryActor {
    type Incoming = DirectoryIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: DirectoryIn) {
        match msg {
            DirectoryIn::Register(claim) => self.register(claim),
            DirectoryIn::Gossip(batch) => self.merge_batch(batch.claims),
            DirectoryIn::Membership(change) => self.on_membership(change),
            DirectoryIn::Tick => self.tick(ctx),
            DirectoryIn::Resolve { actor, reply } => {
                let host = self
                    .route_view
                    .read()
                    .expect("route view poisoned")
                    .get(&actor)
                    .copied();
                let _ = ctx.send(reply, Located { actor, host });
            }
        }
    }
}
