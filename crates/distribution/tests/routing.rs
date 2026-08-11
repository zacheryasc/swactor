//! Directory and actor-address routing contracts.
//!
//! These tests cover the signed directory as the source of actor-address-to-host truth and the use
//! of its route view for best-effort application-message delivery.
//!
//! Behavioral/correctness guarantees:
//! - The directory is the signed, convergent source of actor-address-to-host truth.
//! - Only valid host-signed claims can affect routing.
//! - Actor moves are resolved by generation/conflict rules and converge across peers.
//! - Dead hosts disappear from routable views while retained claims remain recoverable.
//! - Address-only app-message delivery follows the current route view.
//! - Missing, stale, unknown, or dead routes fail as best-effort drops, not panics or false
//!   deliveries.

mod directory_actor {
    //! DirectoryActor convergence and safety: signed claims, supersede, deterministic conflict
    //! resolution, dead-host hiding, catch-up, quieting, and retained recovery claims.

    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, RwLock};

    use swactor::Error;
    use swactor::actor::ActorAddress;
    use swactor::runtime::{Inbox, Runtime, RuntimeConfig, RuntimeParts, SingleThreadRuntime};
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
        dst_rt: Runtime,
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
        rt: Runtime,
        host: RefCell<SingleThreadRuntime>,
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
                let parts = RuntimeParts::new(RuntimeConfig::default())
                    .with_extension(Arc::new(StdExtension::new()));
                let rt = parts.runtime().clone();
                let router = Arc::new(TransportRouter::new());
                rt.set_remote_sink(Arc::new(swactor_transport::CodecRemoteSink::new(
                    codec.clone(),
                    router.clone(),
                )));
                let host = RefCell::new(SingleThreadRuntime::new(parts));

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
                    host,
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
                    node.host.borrow_mut().tick();
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
            self.nodes[observer].host.borrow_mut().tick();
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
    fn long_dead_host_claims_remain_recoverable() {
        // Story: a host's actors are hidden while the host is Dead, but the signed
        // claims are retained even across many directory ticks. A false-dead host
        // must become routable again as soon as SWIM reports it Alive.
        let c = DirectoryCluster::new(2);
        let actor = an_actor();
        c.register(0, c.claim(0, actor, 1));
        assert!(
            c.run_until(200, |c| c.host_in_view(1, actor) == Some(c.ids[0])),
            "precondition: node 1 learns the claim"
        );

        DirectoryCluster::announce(&c.nodes[1], MemberState::Dead, c.ids[0], 2);
        for _ in 0..70 {
            c.round();
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
            "a host returning after many ticks should route from retained claims"
        );
        assert_eq!(
            c.resolve(1, actor),
            Some(c.ids[0]),
            "the retained claim should remain in the directory map"
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
}

mod directory_route_path {
    //! Application delivery through the directory route view: address-only sends, supersede, and
    //! best-effort drops.

    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, RwLock};

    use serde::{Deserialize, Serialize};
    use swactor::Error;
    use swactor::actor::{ActorAddress, ActorInterface};
    use swactor::runtime::{Ctx, Runtime, RuntimeConfig, RuntimeParts, SingleThreadRuntime};
    use swactor::std::StdExtension;
    use swactor_transport::{CodecRegistry, NetworkMessage, TransportRouter};

    use distribution::crypto::{Keypair, KeypairExt};
    use distribution::directory_actor::{DirectoryActor, DirectoryIn};
    use distribution::messages::actor_codec_registry;
    use distribution::swim::actor::MembershipChanged;
    use distribution::transport_bridge::{
        OutFrame, Outbox, OutboxPeerDirectory, OutboxRouteBinder, RouteView, RouteViewTransport,
        peer_addr,
    };
    use distribution::types::{MemberState, NodeId};

    const DIRECTORY_TAG: &str = "swactor_dist::DirectoryGossip";

    // ── A minimal application protocol ──────────────────────────────────────────

    /// An application message. It crosses the wire by actor address alone — the whole
    /// point of §5 — so it carries a registered `type_tag` like any network message.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Hello {
        nonce: u64,
    }

    impl NetworkMessage for Hello {
        fn type_tag() -> &'static str {
            "test::Hello"
        }
    }

    /// An app actor that records every `Hello` it receives (the observable).
    struct AppReceiver {
        seen: Arc<Mutex<Vec<u64>>>,
    }

    impl ActorInterface for AppReceiver {
        type Incoming = Hello;
        type Response = ();
        fn handle(&mut self, _ctx: &Ctx, msg: Hello) {
            self.seen.lock().unwrap().push(msg.nonce);
        }
    }

    /// Local command driving an app actor to send a `Hello` to `to` — by address
    /// alone. This is the §5 caller: it names a target and trusts the directory to
    /// route it.
    #[derive(Clone)]
    struct SendHello {
        to: ActorAddress,
        nonce: u64,
    }

    struct AppSender;

    impl ActorInterface for AppSender {
        type Incoming = SendHello;
        type Response = ();
        fn handle(&mut self, ctx: &Ctx, cmd: SendHello) {
            let _ = ctx.send(cmd.to, Hello { nonce: cmd.nonce });
        }
    }

    // ── The harness ─────────────────────────────────────────────────────────────

    struct RouteNode {
        rt: Runtime,
        host: RefCell<SingleThreadRuntime>,
        outbox: Outbox,
        route_view: RouteView,
        directory: ActorAddress,
        node_id: NodeId,
    }

    struct RouteCluster {
        nodes: Vec<RouteNode>,
        keys: Vec<Keypair>,
        ids: Vec<NodeId>,
        codec: Arc<CodecRegistry>,
    }

    impl RouteCluster {
        fn new(n: usize) -> Self {
            // The shared codec carries the directory's gossip frame *and* the app
            // protocol — the app registers its own type, exactly as a real app would.
            let mut codec = actor_codec_registry();
            codec.register_encoder::<Hello>(|h: &Hello| {
                Ok((
                    Hello::type_tag().to_string(),
                    serde_json::to_vec(h).map_err(|e| Error::from(format!("encode: {e}")))?,
                ))
            });
            codec.register_decoder::<Hello>(Hello::type_tag(), |b: &[u8]| {
                serde_json::from_slice::<Hello>(b).map_err(|e| Error::from(format!("decode: {e}")))
            });
            let codec = Arc::new(codec);

            let keys: Vec<Keypair> = (0..n).map(|_| Keypair::generate()).collect();
            let ids: Vec<NodeId> = keys.iter().map(|k| k.node_id()).collect();

            let mut nodes = Vec::new();
            for &nid in &ids {
                let parts = RuntimeParts::new(RuntimeConfig::default())
                    .with_extension(Arc::new(StdExtension::new()));
                let rt = parts.runtime().clone();
                let router = Arc::new(TransportRouter::new());
                rt.set_remote_sink(Arc::new(swactor_transport::CodecRemoteSink::new(
                    codec.clone(),
                    router.clone(),
                )));
                let host = RefCell::new(SingleThreadRuntime::new(parts));

                let outbox: Outbox = Arc::new(Mutex::new(Vec::new()));
                let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
                // Gossip egress (directory → peer) and app egress (RouteView → host)
                // both feed the one outbox, just like the live driver.
                let peer_directory = Arc::new(OutboxPeerDirectory::new(
                    Arc::clone(&router),
                    Arc::clone(&outbox),
                ));
                let route_view_transport = Arc::new(RouteViewTransport::new(
                    Arc::clone(&route_view),
                    Arc::clone(&outbox),
                ));
                let route_binder = Arc::new(OutboxRouteBinder::new(
                    Arc::clone(&router),
                    Arc::clone(&route_view_transport),
                ));
                let directory = rt
                    .spawn(DirectoryActor::new(
                        nid,
                        peer_directory,
                        Arc::clone(&route_view),
                        route_binder,
                    ))
                    .expect("spawn DirectoryActor");

                nodes.push(RouteNode {
                    rt,
                    host,
                    outbox,
                    route_view,
                    directory,
                    node_id: nid,
                });
            }

            // Every node considers the others alive.
            for i in 0..n {
                for j in 0..n {
                    if i != j {
                        nodes[i]
                            .rt
                            .send_to(
                                nodes[i].directory,
                                DirectoryIn::Membership(MembershipChanged {
                                    node_id: ids[j],
                                    state: MemberState::Alive,
                                    incarnation: 1,
                                }),
                            )
                            .unwrap();
                    }
                }
            }

            let c = RouteCluster {
                nodes,
                keys,
                ids,
                codec,
            };
            c.settle(4);
            c
        }

        /// Move every queued frame to its destination, mirroring the
        /// engine-hosted adapter pump (sender-side outbox drain, then dest-first
        /// delivery). A frame addressed to a node's peer-mailbox is gossip and is
        /// routed by tag; anything else is an app message delivered to its `dest`.
        fn deliver_wire(&self) {
            let mut frames: Vec<OutFrame> = Vec::new();
            for node in &self.nodes {
                frames.extend(node.outbox.lock().unwrap().drain(..));
            }
            for f in frames {
                let Some(dst) = self.nodes.iter().find(|nd| nd.node_id == f.to) else {
                    continue;
                };
                let Ok(boxed) = self.codec.decode(&f.type_tag, &f.payload) else {
                    continue;
                };
                if f.dest == peer_addr(dst.node_id) {
                    // Gossip → tag route (the directory is the only gossip actor here).
                    if f.type_tag == DIRECTORY_TAG {
                        let _ = dst.rt.deliver_raw(dst.directory, boxed);
                    }
                } else {
                    // Application message → deliver straight to the addressed actor.
                    let _ = dst.rt.deliver_raw(f.dest, boxed);
                }
            }
        }

        /// Tick every runtime and flush the wire `k` times — settles mailboxes and
        /// multi-hop deliveries without driving the directory clocks.
        fn settle(&self, k: usize) {
            for _ in 0..k {
                for node in &self.nodes {
                    node.host.borrow_mut().tick();
                }
                self.deliver_wire();
            }
        }

        /// One dissemination round: tick the directory clocks, then settle.
        fn round(&self) {
            for node in &self.nodes {
                let _ = node.rt.send_to(node.directory, DirectoryIn::Tick);
            }
            self.settle(6);
        }

        fn run_until<F: Fn(&RouteCluster) -> bool>(&self, cap: usize, cond: F) -> bool {
            for _ in 0..cap {
                if cond(self) {
                    return true;
                }
                self.round();
            }
            cond(self)
        }

        fn spawn_receiver(&self, node: usize) -> (ActorAddress, Arc<Mutex<Vec<u64>>>) {
            let seen = Arc::new(Mutex::new(Vec::new()));
            let addr = self.nodes[node]
                .rt
                .spawn(AppReceiver {
                    seen: Arc::clone(&seen),
                })
                .expect("spawn AppReceiver");
            (addr, seen)
        }

        fn spawn_sender(&self, node: usize) -> ActorAddress {
            self.nodes[node]
                .rt
                .spawn(AppSender)
                .expect("spawn AppSender")
        }

        /// Author a host claim for `actor` on `host` and begin disseminating it.
        fn register_claim(&self, host: usize, actor: ActorAddress, generation: u64) {
            let claim = self.keys[host].sign_directory_entry(actor, generation);
            self.nodes[host]
                .rt
                .send_to(self.nodes[host].directory, DirectoryIn::Register(claim))
                .unwrap();
        }

        /// Drive `sender` (on `from`) to send a `Hello` to `to` by address alone.
        fn send_hello(&self, from: usize, sender: ActorAddress, to: ActorAddress, nonce: u64) {
            self.nodes[from]
                .rt
                .send_to(sender, SendHello { to, nonce })
                .unwrap();
        }

        fn announce(&self, node: usize, state: MemberState, who: NodeId) {
            self.nodes[node]
                .rt
                .send_to(
                    self.nodes[node].directory,
                    DirectoryIn::Membership(MembershipChanged {
                        node_id: who,
                        state,
                        incarnation: 2,
                    }),
                )
                .unwrap();
        }

        fn host_in_view(&self, observer: usize, actor: ActorAddress) -> Option<NodeId> {
            self.nodes[observer]
                .route_view
                .read()
                .unwrap()
                .get(&actor)
                .copied()
        }
    }

    fn nonces(seen: &Arc<Mutex<Vec<u64>>>) -> Vec<u64> {
        seen.lock().unwrap().clone()
    }

    #[test]
    fn a_message_routes_to_an_actor_by_address_alone() {
        // Story: an app actor on A sends to an actor on B knowing only its address;
        // the directory routes it there. The pure §5 path.
        let c = RouteCluster::new(3);
        let (target, seen) = c.spawn_receiver(1); // hosted on node B
        c.register_claim(1, target, 1);
        let sender = c.spawn_sender(0); // lives on node A

        // A must learn where `target` lives before it can route to it.
        assert!(
            c.run_until(200, |c| c.host_in_view(0, target) == Some(c.ids[1])),
            "node A never learned the target's host"
        );

        c.send_hello(0, sender, target, 42);
        c.settle(10);
        assert_eq!(
            nonces(&seen),
            vec![42],
            "the message did not reach the target by address alone"
        );
    }

    #[test]
    fn a_send_to_an_unknown_actor_is_silently_dropped() {
        // Contract: addressing an actor the directory never learned drops best-effort —
        // no panic, nothing surfaced to the sender, nothing delivered.
        let c = RouteCluster::new(3);
        // A receiver exists on B, but its claim is *never registered*, so no node ever
        // learns where it lives.
        let (unknown, seen) = c.spawn_receiver(1);
        let sender = c.spawn_sender(0);

        // Let the cluster run so it's clearly settled, not merely not-yet-converged.
        for _ in 0..20 {
            c.round();
        }
        assert_eq!(
            c.host_in_view(0, unknown),
            None,
            "an unregistered actor must be unknown"
        );

        c.send_hello(0, sender, unknown, 7);
        c.settle(10);
        assert!(
            nonces(&seen).is_empty(),
            "a send to an unknown actor must not be delivered"
        );
    }

    #[test]
    fn routing_follows_a_supersede() {
        // Story: an actor's claim is superseded to a new host; subsequent sends follow
        // the route to the new host and no longer land on the old one.
        let c = RouteCluster::new(3);
        let (target, on_b) = c.spawn_receiver(1); // the actor really lives on B
        c.register_claim(1, target, 1);
        let sender = c.spawn_sender(0);
        assert!(
            c.run_until(200, |c| c.host_in_view(0, target) == Some(c.ids[1])),
            "precondition: A must first route the target to B"
        );

        // Pre-move: the message lands on the B-hosted actor.
        c.send_hello(0, sender, target, 1);
        c.settle(10);
        assert_eq!(
            nonces(&on_b),
            vec![1],
            "pre-supersede send should reach the original host"
        );

        // A strictly-newer claim, signed by C, moves the actor's route to C.
        c.register_claim(2, target, 2);
        assert!(
            c.run_until(200, |c| c.host_in_view(0, target) == Some(c.ids[2])),
            "the route did not follow the higher-generation claim to C"
        );

        // Post-move: the send is routed to C (where no such actor exists) and so no
        // longer reaches the B-hosted actor.
        c.send_hello(0, sender, target, 2);
        c.settle(10);
        assert_eq!(
            nonces(&on_b),
            vec![1],
            "a superseded route must stop landing on the old host"
        );
    }

    #[test]
    fn a_send_to_a_dead_host_drops() {
        // Contract: once the target's host is declared Dead, the actor leaves the route
        // view and sends to it drop — the blind best-effort miss (the route stays
        // registered, but the view no longer resolves a host).
        let c = RouteCluster::new(3);
        let (target, seen) = c.spawn_receiver(1);
        c.register_claim(1, target, 1);
        let sender = c.spawn_sender(0);
        assert!(
            c.run_until(200, |c| c.host_in_view(0, target) == Some(c.ids[1])),
            "precondition: A must first route the target to its host"
        );
        c.send_hello(0, sender, target, 1);
        c.settle(10);
        assert_eq!(
            nonces(&seen),
            vec![1],
            "precondition: the live-host send should arrive"
        );

        // The host dies (from A's perspective); the target leaves A's route view.
        c.announce(0, MemberState::Dead, c.ids[1]);
        c.settle(4);
        assert_eq!(
            c.host_in_view(0, target),
            None,
            "a dead host's actor must leave the route view"
        );

        c.send_hello(0, sender, target, 2);
        c.settle(10);
        assert_eq!(nonces(&seen), vec![1], "a send to a dead host must drop");
    }
}
