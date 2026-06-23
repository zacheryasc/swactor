//! Behavioral tests for the `DirectoryActor` (`DIRECTORY.md`) over a multi-runtime
//! mesh — the standalone directory in isolation, before any live-node wiring.
//!
//! These prove the directory's contract by its **published observables only** —
//! the `RouteView` (`ActorAddress → NodeId`) each node publishes, and the `Resolve`
//! reply — never `map`/`hot`/`cursor`. Claims travel as their own `DirectoryGossip`
//! frames over the real codec / `TransportRouter` / `deliver_raw` path, exactly as
//! they will in production. Eventuality is pinned by converge-or-timeout, never a
//! fixed tick count.
//!
//! Node identities are real keypairs: a `DirectoryEntry` is signed over
//! `(actor, host, generation)` and verified against its `host` on merge, so a
//! cluster of synthetic `[i; 32]` ids could never produce a verifiable claim.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use swactor::Error;
use swactor::actor::ActorAddress;
use swactor::runtime::{Inbox, Runtime, RuntimeConfig};
use swactor::std::StdExtension;
use swactor_transport::{CodecRegistry, Transport, TransportRouter, WireEnvelope};

use distribution::crypto::{Keypair, KeypairExt};
use distribution::directory_actor::{DirectoryActor, DirectoryIn, Located};
use distribution::messages::actor_codec_registry;
use distribution::swim::actor::{MembershipChanged, SharedPeerDirectory};
use distribution::transport_bridge::{NoopRouteBinder, RouteView, peer_addr};
use distribution::types::{DirectoryEntry, MemberState, NodeId};

const DIRECTORY_TAG: &str = "swactor_dist::DirectoryGossip";

/// Carries an encoded frame into the destination runtime and performs the
/// production ingress: decode, then `deliver_raw` to the local actor that owns the
/// frame's `type_tag`. Counts every frame it carries, so a test can prove a
/// settled cluster has gone quiet.
struct Link {
    dst_rt: Arc<Runtime>,
    routes: HashMap<String, ActorAddress>,
    codec: Arc<CodecRegistry>,
    frames: Arc<AtomicUsize>,
}

impl Transport for Link {
    fn send(&self, wire: WireEnvelope) -> Result<(), Error> {
        let addr = *self
            .routes
            .get(&wire.type_tag)
            .ok_or_else(|| Error::from(format!("no local actor for tag {}", wire.type_tag)))?;
        let msg = self.codec.decode(&wire.type_tag, &wire.payload)?;
        self.frames.fetch_add(1, Ordering::Relaxed);
        self.dst_rt.deliver_raw(addr, msg)
    }
}

/// One node: a runtime hosting a `DirectoryActor`, plus the shared state needed to
/// wire it into a mesh and observe it.
struct Node {
    rt: Arc<Runtime>,
    directory: ActorAddress,
    dir: SharedPeerDirectory,
    router: Arc<TransportRouter>,
    route_view: RouteView,
}

struct DirectoryCluster {
    nodes: Vec<Node>,
    keys: Vec<Keypair>,
    ids: Vec<NodeId>,
    frames: Arc<AtomicUsize>,
}

impl DirectoryCluster {
    fn new(n: usize) -> Self {
        let codec = Arc::new(actor_codec_registry());
        let keys: Vec<Keypair> = (0..n).map(|_| Keypair::generate()).collect();
        let ids: Vec<NodeId> = keys.iter().map(|k| k.node_id()).collect();
        let frames = Arc::new(AtomicUsize::new(0));

        let mut nodes = Vec::new();
        for &nid in &ids {
            let mut rt = Runtime::new(RuntimeConfig::default())
                .with_extension(Arc::new(StdExtension::new()));
            let router = Arc::new(TransportRouter::new());
            rt.set_remote_sink(Arc::new(swactor_transport::CodecRemoteSink::new(
                codec.clone(),
                router.clone(),
            )));
            let rt = Arc::new(rt);

            let dir = SharedPeerDirectory::new();
            let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
            let directory = rt
                .spawn(DirectoryActor::new(
                    nid,
                    Arc::new(dir.clone()),
                    route_view.clone(),
                    Arc::new(NoopRouteBinder),
                ))
                .expect("spawn DirectoryActor");

            nodes.push(Node {
                rt,
                directory,
                dir,
                router,
                route_view,
            });
        }

        // Mesh: bind every peer's NodeId to its synthetic address, route that
        // address through a Link that tag-dispatches into the peer's directory
        // actor, and tell each node the others are Alive.
        for i in 0..n {
            for j in 0..n {
                if i == j {
                    continue;
                }
                Self::link(&nodes, &ids, &frames, &codec, i, j);
                Self::announce(&nodes[i], MemberState::Alive, ids[j], 1);
            }
        }

        let c = DirectoryCluster {
            nodes,
            keys,
            ids,
            frames,
        };
        c.pump(4); // settle membership
        c
    }

    /// Wire node `i`'s egress to peer `j`: a Link carrying directory frames into
    /// `j`'s directory actor, reachable at `j`'s synthetic address.
    fn link(
        nodes: &[Node],
        ids: &[NodeId],
        frames: &Arc<AtomicUsize>,
        codec: &Arc<CodecRegistry>,
        i: usize,
        j: usize,
    ) {
        let syn = peer_addr(ids[j]);
        nodes[i].dir.bind(ids[j], syn, 0);
        let mut routes = HashMap::new();
        routes.insert(DIRECTORY_TAG.to_string(), nodes[j].directory);
        nodes[i].router.add_route(
            syn,
            Arc::new(Link {
                dst_rt: nodes[j].rt.clone(),
                routes,
                codec: codec.clone(),
                frames: frames.clone(),
            }),
        );
    }

    fn announce(node: &Node, state: MemberState, who: NodeId, incarnation: u64) {
        node.rt
            .send_to(
                node.directory,
                DirectoryIn::Membership(MembershipChanged {
                    node_id: who,
                    state,
                    incarnation,
                }),
            )
            .unwrap();
    }

    fn pump(&self, k: usize) {
        for _ in 0..k {
            for node in &self.nodes {
                node.rt.tick();
            }
        }
    }

    /// One dissemination round: tick every directory clock, then settle deliveries.
    fn round(&self) {
        for node in &self.nodes {
            let _ = node.rt.send_to(node.directory, DirectoryIn::Tick);
        }
        self.pump(6);
    }

    fn run_until<F: Fn(&DirectoryCluster) -> bool>(&self, cap: usize, cond: F) -> bool {
        if cond(self) {
            return true;
        }
        for _ in 0..cap {
            self.round();
            if cond(self) {
                return true;
            }
        }
        false
    }

    /// A claim authored by node `i` for `actor` at `generation` — signed by node
    /// `i`'s key, so its host is `ids[i]` and it verifies.
    fn claim(&self, i: usize, actor: ActorAddress, generation: u64) -> DirectoryEntry {
        self.keys[i].sign_directory_entry(actor, generation)
    }

    /// Author/refresh a claim on node `i` (the host) and begin disseminating it.
    fn register(&self, i: usize, claim: DirectoryEntry) {
        self.nodes[i]
            .rt
            .send_to(self.nodes[i].directory, DirectoryIn::Register(claim))
            .unwrap();
    }

    /// The host `observer` currently routes `actor` to, read straight from its
    /// published `RouteView` (the load-bearing observable).
    fn host_in_view(&self, observer: usize, actor: ActorAddress) -> Option<NodeId> {
        self.nodes[observer]
            .route_view
            .read()
            .unwrap()
            .get(&actor)
            .copied()
    }

    /// The host `observer` resolves `actor` to via the diagnostic `Resolve` reply
    /// (the other observable).
    fn resolve(&self, observer: usize, actor: ActorAddress) -> Option<NodeId> {
        let inbox: Inbox<Located> = self.nodes[observer].rt.new_inbox().unwrap();
        self.nodes[observer]
            .rt
            .send_to(
                self.nodes[observer].directory,
                DirectoryIn::Resolve {
                    actor,
                    reply: *inbox.addr(),
                },
            )
            .unwrap();
        self.nodes[observer].rt.tick();
        inbox.try_recv().and_then(|located| located.host)
    }

    fn frames(&self) -> usize {
        self.frames.load(Ordering::Relaxed)
    }
}

fn an_actor() -> ActorAddress {
    ActorAddress::new_random()
}

#[test]
fn a_claim_converges_to_every_peer() {
    // Story: what one node hosts, every other node comes to know.
    let c = DirectoryCluster::new(3);
    let actor = an_actor();
    c.register(0, c.claim(0, actor, 1));

    let converged = c.run_until(200, |c| {
        (0..c.ids.len()).all(|o| c.host_in_view(o, actor) == Some(c.ids[0]))
    });
    assert!(
        converged,
        "claim did not converge to every peer's RouteView"
    );

    // The diagnostic Resolve reply agrees with the route view.
    assert_eq!(c.resolve(2, actor), Some(c.ids[0]));
}

#[test]
fn a_higher_generation_supersedes_a_move() {
    // Story: an actor moves hosts; the higher-generation claim wins everywhere.
    let c = DirectoryCluster::new(3);
    let actor = an_actor();
    c.register(0, c.claim(0, actor, 1));
    assert!(
        c.run_until(200, |c| (0..3)
            .all(|o| c.host_in_view(o, actor) == Some(c.ids[0]))),
        "precondition: claim must first converge to node 0"
    );

    // The actor is now hosted on node 1, which signs a strictly-newer claim.
    c.register(1, c.claim(1, actor, 2));
    let moved = c.run_until(200, |c| {
        (0..c.ids.len()).all(|o| c.host_in_view(o, actor) == Some(c.ids[1]))
    });
    assert!(
        moved,
        "the higher-generation move did not supersede everywhere"
    );
}

#[test]
fn merge_is_order_and_duplication_immune() {
    // Property: the converged RouteView is independent of the order and
    // multiplicity in which claims are merged. Two sibling nodes, the same alive
    // set, fed the same claims in different orders (one with a duplicate), reach
    // an identical RouteView.
    let c = DirectoryCluster::new(4);
    let (a1, a2, a3) = (an_actor(), an_actor(), an_actor());
    // Hosts 0,1,2 author claims; nodes 0 and 1 are the two observers (both already
    // consider 0,1,2,3 alive from the mesh setup).
    let c1 = c.claim(0, a1, 7);
    let c2 = c.claim(1, a2, 3);
    let c3 = c.claim(2, a3, 5);

    // Node 0 merges in one order; node 1 in another, with a duplicate.
    feed_gossip(&c.nodes[0], vec![c1.clone(), c2.clone(), c3.clone()]);
    feed_gossip(
        &c.nodes[1],
        vec![c3.clone(), c1.clone(), c2.clone(), c3.clone()],
    );
    c.pump(4);

    let view0 = c.nodes[0].route_view.read().unwrap().clone();
    let view1 = c.nodes[1].route_view.read().unwrap().clone();
    assert_eq!(
        view0, view1,
        "merge order / duplication changed the converged view"
    );
    // And it is the expected map, not merely equal-but-empty.
    assert_eq!(view0.get(&a1), Some(&c.ids[0]));
    assert_eq!(view0.get(&a2), Some(&c.ids[1]));
    assert_eq!(view0.get(&a3), Some(&c.ids[2]));
}

#[test]
fn a_settled_cluster_goes_quiet() {
    // Property: once every node has a claim, dissemination stops — steady-state
    // traffic for a stable entry is zero.
    let c = DirectoryCluster::new(3);
    let actor = an_actor();
    c.register(0, c.claim(0, actor, 1));
    assert!(
        c.run_until(200, |c| (0..3)
            .all(|o| c.host_in_view(o, actor) == Some(c.ids[0]))),
        "precondition: must converge before measuring quiescence"
    );

    // Drain any residual dissemination budget.
    for _ in 0..40 {
        c.round();
    }
    let settled = c.frames();
    for _ in 0..20 {
        c.round();
    }
    assert_eq!(
        c.frames(),
        settled,
        "a settled cluster kept emitting directory gossip"
    );
}

#[test]
fn a_rejoining_peer_is_caught_up() {
    // Story: a peer that was Dead during a registration is caught up when it
    // returns — its host re-arms held claims on the rejoin.
    let c = DirectoryCluster::new(3);
    let first = an_actor();
    c.register(0, c.claim(0, first, 1));
    assert!(
        c.run_until(200, |c| c.host_in_view(2, first) == Some(c.ids[0])),
        "precondition: node 2 must first learn the original claim"
    );

    // Node 2 falls out of the alive set of nodes 0 and 1.
    DirectoryCluster::announce(&c.nodes[0], MemberState::Dead, c.ids[2], 2);
    DirectoryCluster::announce(&c.nodes[1], MemberState::Dead, c.ids[2], 2);
    c.pump(4);

    // A new claim is registered while node 2 is away; it cannot reach node 2.
    let late = an_actor();
    c.register(0, c.claim(0, late, 1));
    for _ in 0..20 {
        c.round();
    }
    assert_eq!(
        c.host_in_view(2, late),
        None,
        "a dead peer should not receive claims registered in its absence"
    );

    // Node 2 returns; its hosts re-arm and catch it up on what it missed.
    DirectoryCluster::announce(&c.nodes[0], MemberState::Alive, c.ids[2], 3);
    DirectoryCluster::announce(&c.nodes[1], MemberState::Alive, c.ids[2], 3);
    let caught_up = c.run_until(200, |c| c.host_in_view(2, late) == Some(c.ids[0]));
    assert!(
        caught_up,
        "a rejoining peer was not caught up on missed claims"
    );
}

#[test]
fn a_dead_host_drops_out_of_routing() {
    // Story: when a host dies, its actors stop being routable — they vanish from
    // every surviving peer's RouteView (even before GC reclaims the claim).
    let c = DirectoryCluster::new(3);
    let actor = an_actor();
    c.register(0, c.claim(0, actor, 1));
    assert!(
        c.run_until(200, |c| (1..3)
            .all(|o| c.host_in_view(o, actor) == Some(c.ids[0]))),
        "precondition: peers must first route the actor to its host"
    );

    // Node 0 (the host) is declared Dead on the survivors.
    DirectoryCluster::announce(&c.nodes[1], MemberState::Dead, c.ids[0], 2);
    DirectoryCluster::announce(&c.nodes[2], MemberState::Dead, c.ids[0], 2);
    c.pump(4);

    assert_eq!(
        c.host_in_view(1, actor),
        None,
        "a dead host must drop out of routing"
    );
    assert_eq!(
        c.host_in_view(2, actor),
        None,
        "a dead host must drop out of routing"
    );
}

#[test]
fn an_empty_cluster_loses_nothing() {
    // Contract: with no alive peer, a registered claim is held (not lost) and emits
    // nothing into the void; once a peer appears, it converges.
    let c = DirectoryCluster::new(2);
    // Tear down the alive view on node 0 so it is effectively alone.
    DirectoryCluster::announce(&c.nodes[0], MemberState::Dead, c.ids[1], 5);
    c.pump(4);

    let actor = an_actor();
    c.register(0, c.claim(0, actor, 1));
    let before = c.frames();
    for _ in 0..20 {
        let _ = c.nodes[0]
            .rt
            .send_to(c.nodes[0].directory, DirectoryIn::Tick);
        c.pump(2);
    }
    assert_eq!(
        c.frames(),
        before,
        "a node with no alive peer must emit nothing"
    );
    // The claim survived locally (node 0 still routes its own actor).
    assert_eq!(c.host_in_view(0, actor), Some(c.ids[0]));

    // A peer appears; the held claim converges to it.
    DirectoryCluster::announce(&c.nodes[0], MemberState::Alive, c.ids[1], 6);
    let converged = c.run_until(200, |c| c.host_in_view(1, actor) == Some(c.ids[0]));
    assert!(
        converged,
        "the held claim did not converge once a peer appeared"
    );
}

#[test]
fn a_forged_claim_is_rejected() {
    // Contract: a claim whose signature does not match the host it names is
    // dropped — it never appears in any RouteView, while a genuine claim does.
    let c = DirectoryCluster::new(3);

    // Forge: sign for `victim_actor` with an attacker key, then relabel the host
    // as node 0 — the signature no longer matches node 0's key.
    let attacker = Keypair::generate();
    let victim_actor = an_actor();
    let mut forged = attacker.sign_directory_entry(victim_actor, 9);
    forged.node_id = c.ids[0];

    // Inject the forgery straight into node 1's merge path, alongside a genuine claim.
    let honest_actor = an_actor();
    feed_gossip(&c.nodes[1], vec![forged]);
    c.register(0, c.claim(0, honest_actor, 1));

    let honest_converged = c.run_until(200, |c| {
        (0..3).all(|o| c.host_in_view(o, honest_actor) == Some(c.ids[0]))
    });
    assert!(honest_converged, "the genuine claim should still converge");

    // The forged claim is nowhere — no node ever routed it.
    for o in 0..c.ids.len() {
        assert_eq!(
            c.host_in_view(o, victim_actor),
            None,
            "a forged claim must never enter the route view"
        );
    }
}

#[test]
fn gc_reclaims_a_long_dead_hosts_claims() {
    // Story: a host's claims survive a *brief* death — held, and routable again the
    // moment it returns — but once it has been gone past the GC grace window they
    // are reclaimed, so a long-absent host is NOT resurrected from a stale claim on
    // return. "Hidden" (brief) and "reclaimed" (long) look identical while the host
    // is down; they diverge only on its return, which is the observable face of gc.
    let c = DirectoryCluster::new(2);
    let actor = an_actor();
    c.register(0, c.claim(0, actor, 1));
    assert!(
        c.run_until(200, |c| c.host_in_view(1, actor) == Some(c.ids[0])),
        "precondition: node 1 learns the claim"
    );

    // Brief death, within the grace window: hidden while down, still held, so it
    // routes again as soon as the host is back.
    DirectoryCluster::announce(&c.nodes[1], MemberState::Dead, c.ids[0], 2);
    for _ in 0..5 {
        c.round(); // a few gc ticks, well under GC_GRACE_TICKS
    }
    assert_eq!(
        c.host_in_view(1, actor),
        None,
        "a dead host is hidden from routing"
    );
    DirectoryCluster::announce(&c.nodes[1], MemberState::Alive, c.ids[0], 3);
    c.pump(4);
    assert_eq!(
        c.host_in_view(1, actor),
        Some(c.ids[0]),
        "a host back within the grace window is still held, so it routes again"
    );

    // Long death, past the grace window: each `round` delivers one directory Tick,
    // so >GC_GRACE_TICKS rounds reclaim the claim.
    DirectoryCluster::announce(&c.nodes[1], MemberState::Dead, c.ids[0], 4);
    for _ in 0..70 {
        c.round();
    }
    // On return it is NOT resurrected — the claim was reclaimed, not merely hidden.
    DirectoryCluster::announce(&c.nodes[1], MemberState::Alive, c.ids[0], 5);
    c.pump(4);
    assert_eq!(
        c.host_in_view(1, actor),
        None,
        "a host gone past the GC grace window is reclaimed, not resurrected on return"
    );
    assert_eq!(
        c.resolve(1, actor),
        None,
        "the reclaimed claim is gone from the map too"
    );
}

#[test]
fn equal_generation_ties_break_deterministically() {
    // Property: two claims for the SAME actor at the SAME generation but different
    // hosts must resolve to the SAME winner on every node, regardless of the order
    // each node merges them. The (generation, node_id) tie-break (DIRECTORY.md §4.3)
    // is what stops a double-registration from splitting the directory's view.
    let c = DirectoryCluster::new(4);
    let actor = an_actor();
    let from_2 = c.claim(2, actor, 5); // host = ids[2]
    let from_3 = c.claim(3, actor, 5); // host = ids[3], SAME generation
    // The deterministic winner is the larger node_id.
    let winner = if c.ids[2] > c.ids[3] {
        c.ids[2]
    } else {
        c.ids[3]
    };

    // Two observers merge the rival claims in opposite orders.
    feed_gossip(&c.nodes[0], vec![from_2.clone(), from_3.clone()]);
    feed_gossip(&c.nodes[1], vec![from_3.clone(), from_2.clone()]);
    c.pump(4);

    assert_eq!(c.host_in_view(0, actor), Some(winner));
    assert_eq!(c.host_in_view(1, actor), Some(winner));
    assert_eq!(
        c.host_in_view(0, actor),
        c.host_in_view(1, actor),
        "equal-generation claims must converge to the same winner on every node"
    );
}

/// Deliver a batch of claims straight into a node's directory actor as if gossiped
/// by a peer (exercises the merge path without the round-robin scheduler).
fn feed_gossip(node: &Node, claims: Vec<DirectoryEntry>) {
    use distribution::messages::DirectoryGossip;
    node.rt
        .send_to(
            node.directory,
            DirectoryIn::Gossip(DirectoryGossip { claims }),
        )
        .unwrap();
}
