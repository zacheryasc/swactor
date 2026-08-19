//! SWIM actor/runtime contracts.
//!
//! These tests verify that the actor shell preserves SWIM semantics when time, delivery, failures,
//! and subscriptions are expressed as runtime messages.
//!
//! Behavioral/correctness guarantees:
//! - The actor shell preserves SWIM semantics while replacing direct calls with runtime messages.
//! - Time, send failure, peer binding, and subscriptions enter through explicit actor
//!   messages/seams.
//! - Actor subscribers receive a complete ordered membership-change stream.
//! - Runtime and transport execution detect real unreachability rather than relying on direct
//!   state mutation.
//! - SWIM actors converge across both shared-runtime and separate-runtime transport setups.

mod single_runtime_actor {
    //! SwimActor behavior in one runtime: subscription stream convergence and genuine unreachable-
    //! peer death detection.

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use swactor::runtime::{Runtime, RuntimeConfig, RuntimeParts};
    use swactor::std::StdExtension;
    use swactor_engine::{Engine, SteppingBackend};

    use distribution::swim::actor::{
        MembershipChanged, PeerDirectory, SharedPeerDirectory, SwimActor, SwimIn,
    };
    use distribution::swim::member_list::MemberList;
    use distribution::swim::probe::{ProbeMode, SwimConfig};
    use distribution::types::{MemberState, NodeId};

    const TICK: Duration = Duration::from_millis(10);

    fn id(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    /// A brisk Periodic config so detection completes in a manageable number of
    /// rounds: detection ≈ 2·probe_timeout (direct+indirect) + suspicion_timeout.
    fn brisk_config() -> SwimConfig {
        SwimConfig {
            probe_interval: TICK,
            probe_timeout: TICK * 2,
            indirect_probes: 2,
            suspicion_timeout: TICK * 3,
            dead_reprobe_interval: Duration::ZERO,
            probe_mode: ProbeMode::Periodic,
            lifeguard: None,
        }
    }

    /// A cluster of `n` `SwimActor`s in one runtime, each with its own
    /// `MembershipChanged` subscriber inbox, and a shared Binding.
    struct ActorCluster {
        rt: Runtime,
        _engine: Engine,
        backend: SteppingBackend,
        ids: Vec<NodeId>,
        addrs: Vec<swactor::actor::ActorAddress>,
        inboxes: Vec<swactor::runtime::Inbox<MembershipChanged>>,
        dir: SharedPeerDirectory,
        streams: Vec<Vec<MembershipChanged>>,
        clock: Instant,
        step: u64,
    }

    impl ActorCluster {
        /// Spawn `n` actors. Nodes `1..n` join via node 0 (the seed). Returns once
        /// the join requests have been issued (not yet converged).
        fn new(n: usize) -> Self {
            let parts = RuntimeParts::new(RuntimeConfig::default())
                .with_extension(Arc::new(StdExtension::new()));
            let rt = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let engine = Engine::new(parts, backend.clone()).expect("create stepping actor engine");
            let dir = SharedPeerDirectory::new();
            let now = Instant::now();
            let ids: Vec<NodeId> = (0..n).map(|i| id(i as u8)).collect();

            let mut addrs = Vec::new();
            let mut inboxes = Vec::new();
            for &nid in &ids {
                let addr = rt
                    .spawn(SwimActor::new(
                        nid,
                        brisk_config(),
                        now,
                        Arc::new(dir.clone()),
                    ))
                    .expect("spawn SwimActor");
                // Generation 0 binding; the actor resolves NodeId→ActorAddress here.
                dir.bind(nid, addr, 0);
                let inbox = rt.new_inbox::<MembershipChanged>().expect("inbox");
                rt.send_to(
                    addr,
                    SwimIn::Subscribe {
                        observer: *inbox.addr(),
                    },
                )
                .unwrap();
                addrs.push(addr);
                inboxes.push(inbox);
            }
            let mut c = ActorCluster {
                rt,
                _engine: engine,
                backend,
                ids,
                addrs,
                inboxes,
                dir,
                streams: vec![Vec::new(); n],
                clock: now,
                step: 0,
            };
            // Settle the Subscribe messages.
            c.pump(4);
            // Nodes 1..n join via the seed (node 0).
            let seed = c.ids[0];
            for i in 1..n {
                let addr = c.addrs[i];
                c.rt.send_to(addr, SwimIn::Join { seeds: vec![seed] })
                    .unwrap();
            }
            c.pump(8);
            c.drain();
            c
        }

        fn pump(&self, n: usize) {
            for _ in 0..n {
                self.backend.step();
            }
        }

        /// Drain every inbox into its per-node `MembershipChanged` stream.
        fn drain(&mut self) {
            for (i, inbox) in self.inboxes.iter().enumerate() {
                while let Some(m) = inbox.try_recv() {
                    self.streams[i].push(m);
                }
            }
        }

        /// Advance the clock and deliver a `Tick{now}` to every actor whose NodeId
        /// is still bound (an unbound node is unreachable — genuinely silent). Then
        /// pump the runtime so the resulting sends/acks/gossip settle, and drain.
        fn round(&mut self) {
            self.step += 1;
            self.clock += TICK;
            let now = self.clock;
            for i in 0..self.ids.len() {
                if self.dir.resolve(&self.ids[i]).is_some() {
                    let _ = self.rt.send_to(self.addrs[i], SwimIn::Tick { now });
                }
            }
            // Enough internal ticks for a ping→ack and a relay chain plus the
            // notification hop to settle within one round.
            self.pump(8);
            self.drain();
        }

        fn run_until<F: Fn(&ActorCluster) -> bool>(&mut self, cap: usize, cond: F) -> bool {
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

        /// Reconstruct node `observer`'s membership view by folding its
        /// `MembershipChanged` stream under the §7 merge rule (the Goal-7 fold).
        fn folded_view(&self, observer: usize) -> BTreeMap<[u8; 32], MemberState> {
            // A sentinel self-id that never appears in any stream, so every entry is
            // stored (self is never stored, §7 inv. 4).
            let mut ml = MemberList::new(id(250));
            for m in &self.streams[observer] {
                ml.apply(m.node_id, m.state, m.incarnation);
            }
            ml.all_members()
                .iter()
                .map(|e| (e.node_id.0, e.state))
                .collect()
        }

        /// Does `observer`'s folded view show `subject` in `state`?
        fn sees(&self, observer: usize, subject: NodeId, state: MemberState) -> bool {
            self.folded_view(observer).get(&subject.0) == Some(&state)
        }

        /// Every node's folded stream shows every other node Alive.
        fn all_converged_alive(&self) -> bool {
            let n = self.ids.len();
            (0..n).all(|o| (0..n).all(|s| o == s || self.sees(o, id(s as u8), MemberState::Alive)))
        }
    }

    #[test]
    fn actors_converge_to_a_shared_alive_view_via_the_membership_changed_stream() {
        // §6.3 + §3.2 + §4.2: actors that join through the seed reach a shared Alive
        // view, observed solely through the MembershipChanged notification stream
        // each subscriber receives — the actorization seams carry gossip end to end.
        let mut c = ActorCluster::new(4);
        assert!(
            c.run_until(400, |c| c.all_converged_alive()),
            "actors did not converge to a shared Alive view through MembershipChanged"
        );
    }

    #[test]
    fn an_unreachable_actor_is_detected_dead_by_survivors() {
        // §4.3 + §9: once a node becomes unreachable (its Binding is dropped, so a
        // survivor's probe genuinely never gets an Ack), the survivors must converge
        // on it being Dead — real detection driven by SendFailed + probe timeout,
        // not an injected death.
        let mut c = ActorCluster::new(4);
        assert!(
            c.run_until(400, |c| c.all_converged_alive()),
            "precondition: must converge"
        );

        // Genuinely silence node 3: drop its binding so nobody can deliver to it, and
        // stop ticking it (round() skips unbound nodes).
        let dead = 3usize;
        c.dir.unbind(&id(dead as u8));

        let detected = c.run_until(2000, |c| {
            (0..4).all(|o| o == dead || c.sees(o, id(dead as u8), MemberState::Dead))
        });
        assert!(
            detected,
            "survivors must converge on the unreachable node being Dead"
        );
    }
}

mod transport_runtime_actor {
    //! SwimActor behavior across separate runtimes through codec, TransportRouter, deliver_raw, and
    //! transport send failure.

    use std::collections::{BTreeMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    use swactor::Error;
    use swactor::actor::ActorAddress;
    use swactor::runtime::{Inbox, Runtime, RuntimeConfig, RuntimeParts};
    use swactor::std::StdExtension;
    use swactor_engine::{Engine, SteppingBackend};
    use swactor_transport::{CodecRegistry, Transport, TransportRouter, WireEnvelope};

    use distribution::messages::actor_codec_registry;
    use distribution::swim::actor::{MembershipChanged, SharedPeerDirectory, SwimActor, SwimIn};
    use distribution::swim::member_list::MemberList;
    use distribution::swim::probe::{ProbeMode, SwimConfig};
    use distribution::types::{MemberState, NodeId};

    const TICK: Duration = Duration::from_millis(10);

    fn id(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    /// The deterministic, reversible `NodeId → ActorAddress` mapping the production
    /// `PeerBinder` will use: a remote peer's mailbox address is its node id's bytes.
    /// Guaranteed non-local on every other runtime, so `ctx.send` takes the transport
    /// path; never collides with a locally `new_random()`-addressed actor.
    fn peer_addr(node: NodeId) -> ActorAddress {
        ActorAddress(node.0)
    }

    /// Brisk Periodic config (mirrors the in-process harness) so detection completes
    /// in a manageable number of rounds.
    fn brisk_config() -> SwimConfig {
        SwimConfig {
            probe_interval: TICK,
            probe_timeout: TICK * 2,
            indirect_probes: 2,
            suspicion_timeout: TICK * 3,
            dead_reprobe_interval: Duration::ZERO,
            probe_mode: ProbeMode::Periodic,
            lifeguard: None,
        }
    }

    /// Stands in for a network transport: serializes nothing itself (the runtime
    /// already encoded to a `WireEnvelope`), it just carries the frame into the
    /// destination runtime and performs the production ingress.
    /// A `partition` set models nodes that have fallen off the network: a frame whose
    /// source or destination is partitioned is dropped (genuine silence).
    struct Link {
        src: usize,
        dst: usize,
        dst_rt: Runtime,
        dst_swim: ActorAddress,
        codec: Arc<CodecRegistry>,
        partition: Arc<Mutex<HashSet<usize>>>,
    }

    impl Transport for Link {
        fn send(&self, wire: WireEnvelope) -> Result<(), Error> {
            {
                let p = self.partition.lock().unwrap();
                if p.contains(&self.src) || p.contains(&self.dst) {
                    // Unreachable: the egress fails, which the SwimActor turns into
                    // SendFailed{to} (§4.3) — exactly how a real dropped connection
                    // feeds failure detection.
                    return Err(Error::from("link partitioned"));
                }
            }
            let msg = self.codec.decode(&wire.type_tag, &wire.payload)?;
            self.dst_rt.deliver_raw(self.dst_swim, msg)
        }
    }

    /// A cluster of `n` `SwimActor`s, each on its **own** runtime, meshed through
    /// `Link` transports — the multi-runtime analog of `swim_actor.rs::ActorCluster`.
    struct TransportCluster {
        rts: Vec<Runtime>,
        _engines: Vec<Engine>,
        backends: Vec<SteppingBackend>,
        swims: Vec<ActorAddress>,
        inboxes: Vec<Inbox<MembershipChanged>>,
        streams: Vec<Vec<MembershipChanged>>,
        ids: Vec<NodeId>,
        partition: Arc<Mutex<HashSet<usize>>>,
        clock: Instant,
    }

    impl TransportCluster {
        fn new(n: usize) -> Self {
            let codec = Arc::new(actor_codec_registry());
            let now = Instant::now();
            let ids: Vec<NodeId> = (0..n).map(|i| id(i as u8)).collect();
            let partition = Arc::new(Mutex::new(HashSet::new()));

            let mut rts = Vec::new();
            let mut engines = Vec::new();
            let mut backends = Vec::new();
            let mut swims = Vec::new();
            let mut dirs = Vec::new();
            let mut routers = Vec::new();
            let mut inboxes = Vec::new();

            // Phase 1: one runtime per node, each with the actor codec registry + an
            // (initially empty) transport router. Spawn the SwimActor and subscribe.
            for &nid in &ids {
                let parts = RuntimeParts::new(RuntimeConfig::default())
                    .with_extension(Arc::new(StdExtension::new()));
                let rt = parts.runtime().clone();
                let router = Arc::new(TransportRouter::new());
                rt.set_remote_sink(Arc::new(swactor_transport::CodecRemoteSink::new(
                    codec.clone(),
                    router.clone(),
                )));
                let backend = SteppingBackend::new();
                let engine =
                    Engine::new(parts, backend.clone()).expect("create stepping actor engine");

                let dir = SharedPeerDirectory::new();
                let swim = rt
                    .spawn(SwimActor::new(
                        nid,
                        brisk_config(),
                        now,
                        Arc::new(dir.clone()),
                    ))
                    .expect("spawn SwimActor");
                // Self resolves to the local mailbox; peers are wired in phase 2.
                dir.bind(nid, swim, 0);
                let inbox = rt.new_inbox::<MembershipChanged>().expect("inbox");
                rt.send_to(
                    swim,
                    SwimIn::Subscribe {
                        observer: *inbox.addr(),
                    },
                )
                .unwrap();

                rts.push(rt);
                engines.push(engine);
                backends.push(backend);
                swims.push(swim);
                dirs.push(dir);
                routers.push(router);
                inboxes.push(inbox);
            }

            // Phase 2: mesh. Each node resolves every peer's NodeId to its synthetic
            // peer address and routes that address through a Link into the peer's
            // runtime handle. Now every cloned handle exists, so the mutual references
            // close cleanly.
            for i in 0..n {
                for j in 0..n {
                    if i == j {
                        continue;
                    }
                    let syn = peer_addr(ids[j]);
                    dirs[i].bind(ids[j], syn, 0);
                    routers[i].add_route(
                        syn,
                        Arc::new(Link {
                            src: i,
                            dst: j,
                            dst_rt: rts[j].clone(),
                            dst_swim: swims[j],
                            codec: codec.clone(),
                            partition: partition.clone(),
                        }),
                    );
                }
            }

            let mut c = TransportCluster {
                rts,
                _engines: engines,
                backends,
                swims,
                inboxes,
                streams: vec![Vec::new(); n],
                ids,
                partition,
                clock: now,
            };
            c.pump(4); // settle Subscribe

            // Nodes 1..n join via the seed (node 0).
            for i in 1..n {
                c.rts[i]
                    .send_to(
                        c.swims[i],
                        SwimIn::Join {
                            seeds: vec![c.ids[0]],
                        },
                    )
                    .unwrap();
            }
            c.pump(12);
            c.drain();
            c
        }

        /// Tick every runtime `k` times. Each cross-runtime hop needs the destination
        /// to tick after the source, so one ping→ack round trip settles in a few
        /// iterations; `k` is sized so a probe + its notification settle per round.
        fn pump(&self, k: usize) {
            for _ in 0..k {
                for backend in &self.backends {
                    backend.step();
                }
            }
        }

        fn drain(&mut self) {
            for (i, inbox) in self.inboxes.iter().enumerate() {
                while let Some(m) = inbox.try_recv() {
                    self.streams[i].push(m);
                }
            }
        }

        /// Advance the clock and deliver `Tick{now}` to every non-partitioned actor
        /// (a partitioned node is genuinely silent), then let the runtimes settle.
        fn round(&mut self) {
            self.clock += TICK;
            let now = self.clock;
            let partitioned = self.partition.lock().unwrap().clone();
            for i in 0..self.ids.len() {
                if !partitioned.contains(&i) {
                    let _ = self.rts[i].send_to(self.swims[i], SwimIn::Tick { now });
                }
            }
            self.pump(12);
            self.drain();
        }

        fn run_until<F: Fn(&TransportCluster) -> bool>(&mut self, cap: usize, cond: F) -> bool {
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

        /// Fold node `observer`'s `MembershipChanged` stream under the §7 merge rule.
        fn folded_view(&self, observer: usize) -> BTreeMap<[u8; 32], MemberState> {
            let mut ml = MemberList::new(id(250)); // sentinel self never appears
            for m in &self.streams[observer] {
                ml.apply(m.node_id, m.state, m.incarnation);
            }
            ml.all_members()
                .iter()
                .map(|e| (e.node_id.0, e.state))
                .collect()
        }

        fn sees(&self, observer: usize, subject: NodeId, state: MemberState) -> bool {
            self.folded_view(observer).get(&subject.0) == Some(&state)
        }

        fn all_converged_alive(&self) -> bool {
            let n = self.ids.len();
            (0..n).all(|o| (0..n).all(|s| o == s || self.sees(o, id(s as u8), MemberState::Alive)))
        }
    }

    #[test]
    fn actors_on_separate_runtimes_converge_alive_over_the_transport() {
        // The gossip that makes them converge crosses the codec/TransportRouter/
        // deliver_raw seam on every hop — proving SwimActor works against genuinely
        // remote peers, not just co-located actors.
        let mut c = TransportCluster::new(3);
        assert!(
            c.run_until(600, |c| c.all_converged_alive()),
            "actors on separate runtimes did not converge Alive over the transport"
        );
    }

    #[test]
    fn a_partitioned_actor_is_detected_dead_over_the_transport() {
        // Once a node is partitioned, survivors' probes to it fail at the transport
        // (Link returns Err → SendFailed), and it can no longer refute — so the
        // survivors must converge on Dead. Real detection over the wire seam.
        let mut c = TransportCluster::new(3);
        assert!(
            c.run_until(600, |c| c.all_converged_alive()),
            "precondition: must converge Alive first"
        );

        let dead = 2usize;
        c.partition.lock().unwrap().insert(dead);

        let detected = c.run_until(3000, |c| {
            (0..3).all(|o| o == dead || c.sees(o, id(dead as u8), MemberState::Dead))
        });
        assert!(
            detected,
            "survivors must converge on the partitioned node being Dead"
        );
    }
}

mod actor_membership_safety_edges {
    //! Actor-observable safety edges: resurrection after silence, multi-hop death dissemination,
    //! and bounded stale-refute behavior.

    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use swactor::runtime::{Runtime, RuntimeConfig, RuntimeParts};
    use swactor::std::StdExtension;
    use swactor_engine::{Engine, SteppingBackend};

    use distribution::swim::actor::{
        MembershipChanged, PeerDirectory, SharedPeerDirectory, SwimActor, SwimIn,
    };
    use distribution::swim::dissemination::{DisseminationQueue, membership_update};
    use distribution::swim::member_list::MemberList;
    use distribution::swim::node::SwimNode;
    use distribution::swim::probe::{ProbeMode, SwimConfig};
    use distribution::types::{MemberState, NodeId};

    const TICK: Duration = Duration::from_millis(10);

    fn id(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    /// Brisk Periodic config with dead-reprobe ENABLED, so the §9.8 partition-heal
    /// detector actually re-probes Dead nodes and can resurrect them.
    fn reprobe_config() -> SwimConfig {
        SwimConfig {
            probe_interval: TICK,
            probe_timeout: TICK * 2,
            indirect_probes: 2,
            suspicion_timeout: TICK * 3,
            dead_reprobe_interval: TICK * 2,
            probe_mode: ProbeMode::Periodic,
            lifeguard: None,
        }
    }

    /// A cluster of `n` `SwimActor`s in one runtime, each with its own
    /// `MembershipChanged` inbox and a shared Binding. Mirrors the shipped harness
    /// but lets the test choose the config and rebind a dropped node.
    struct Cluster {
        rt: Runtime,
        _engine: Engine,
        backend: SteppingBackend,
        ids: Vec<NodeId>,
        addrs: Vec<swactor::actor::ActorAddress>,
        inboxes: Vec<swactor::runtime::Inbox<MembershipChanged>>,
        dir: SharedPeerDirectory,
        streams: Vec<Vec<MembershipChanged>>,
        clock: Instant,
    }

    impl Cluster {
        fn new(n: usize, config: SwimConfig) -> Self {
            let parts = RuntimeParts::new(RuntimeConfig::default())
                .with_extension(Arc::new(StdExtension::new()));
            let rt = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let engine = Engine::new(parts, backend.clone()).expect("create stepping actor engine");
            let dir = SharedPeerDirectory::new();
            let now = Instant::now();
            let ids: Vec<NodeId> = (0..n).map(|i| id(i as u8)).collect();

            let mut addrs = Vec::new();
            let mut inboxes = Vec::new();
            for &nid in &ids {
                let addr = rt
                    .spawn(SwimActor::new(
                        nid,
                        config.clone(),
                        now,
                        Arc::new(dir.clone()),
                    ))
                    .expect("spawn SwimActor");
                dir.bind(nid, addr, 0);
                let inbox = rt.new_inbox::<MembershipChanged>().expect("inbox");
                rt.send_to(
                    addr,
                    SwimIn::Subscribe {
                        observer: *inbox.addr(),
                    },
                )
                .unwrap();
                addrs.push(addr);
                inboxes.push(inbox);
            }
            let mut c = Cluster {
                rt,
                _engine: engine,
                backend,
                ids,
                addrs,
                inboxes,
                dir,
                streams: vec![Vec::new(); n],
                clock: now,
            };
            c.pump(4);
            let seed = c.ids[0];
            for i in 1..n {
                let addr = c.addrs[i];
                c.rt.send_to(addr, SwimIn::Join { seeds: vec![seed] })
                    .unwrap();
            }
            c.pump(8);
            c.drain();
            c
        }

        fn pump(&self, n: usize) {
            for _ in 0..n {
                self.backend.step();
            }
        }

        fn drain(&mut self) {
            for (i, inbox) in self.inboxes.iter().enumerate() {
                while let Some(m) = inbox.try_recv() {
                    self.streams[i].push(m);
                }
            }
        }

        /// Advance the clock and tick every still-bound actor (unbound = silent).
        fn round(&mut self) {
            self.clock += TICK;
            let now = self.clock;
            for i in 0..self.ids.len() {
                if self.dir.resolve(&self.ids[i]).is_some() {
                    let _ = self.rt.send_to(self.addrs[i], SwimIn::Tick { now });
                }
            }
            self.pump(8);
            self.drain();
        }

        fn run_until<F: Fn(&Cluster) -> bool>(&mut self, cap: usize, cond: F) -> bool {
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

        /// Fold observer's `MembershipChanged` stream under the §7 merge rule.
        fn folded_view(&self, observer: usize) -> BTreeMap<[u8; 32], MemberState> {
            let mut ml = MemberList::new(id(250));
            for m in &self.streams[observer] {
                ml.apply(m.node_id, m.state, m.incarnation);
            }
            ml.all_members()
                .iter()
                .map(|e| (e.node_id.0, e.state))
                .collect()
        }

        fn sees(&self, observer: usize, subject: NodeId, state: MemberState) -> bool {
            self.folded_view(observer).get(&subject.0) == Some(&state)
        }

        fn all_converged_alive(&self) -> bool {
            let n = self.ids.len();
            (0..n).all(|o| (0..n).all(|s| o == s || self.sees(o, id(s as u8), MemberState::Alive)))
        }
    }

    #[test]
    fn a_silenced_node_that_returns_is_resurrected_dead_then_alive() {
        // Goal 3 (death is provisional). A node is genuinely silenced (binding
        // dropped, no ticks), detected Dead by survivors, then comes back. The §9.8
        // dead-reprobe must ping it, the §10.6 re-enqueue hands it its own Dead
        // record, it refutes (Alive at a higher incarnation), and that refutation
        // propagates so survivors flip Dead → Alive. The endpoint is asserted, not
        // just post-heal agreement.
        let mut c = Cluster::new(4, reprobe_config());
        assert!(
            c.run_until(400, |c| c.all_converged_alive()),
            "precondition: cluster must converge Alive"
        );

        let victim = 3usize;
        let vid = id(victim as u8);

        // Genuine silence.
        c.dir.unbind(&vid);
        let detected = c.run_until(2000, |c| {
            (0..4).all(|o| o == victim || c.sees(o, vid, MemberState::Dead))
        });
        assert!(
            detected,
            "survivors must first detect the silenced node as Dead"
        );

        // The node returns: rebind at a higher generation, ticking resumes.
        c.dir.bind(vid, c.addrs[victim], 1);

        let resurrected = c.run_until(4000, |c| {
            (0..4).all(|o| o == victim || c.sees(o, vid, MemberState::Alive))
        });
        assert!(
            resurrected,
            "a returned node must be re-probed and resurrected Dead -> Alive (Goal 3)"
        );
    }

    #[test]
    fn a_single_observed_death_infects_a_non_probing_witness() {
        // Goal 6 (dissemination reaches everyone). After convergence, silence one
        // node. SOME survivor's probe times out and originates the Dead. Every OTHER
        // survivor — including ones whose own probe round may never have targeted the
        // dead node before it was evicted from their alive set — must still converge
        // on Dead, which is only reachable by multi-hop piggyback infection (§11),
        // not direct observation.
        let mut c = Cluster::new(5, reprobe_config());
        assert!(
            c.run_until(400, |c| c.all_converged_alive()),
            "precondition: converge"
        );

        let victim = 4usize;
        let vid = id(victim as u8);
        c.dir.unbind(&vid);

        let everyone_knows = c.run_until(3000, |c| {
            (0..5).all(|o| o == victim || c.sees(o, vid, MemberState::Dead))
        });
        assert!(
            everyone_knows,
            "every survivor must learn the death via gossip infection, not just the prober (Goal 6)"
        );
    }

    // ─── §7.1 storm bound, end to end (no actor harness needed) ──────────────────

    fn flood_piggyback(target: NodeId, state: MemberState, incarnation: u64) -> Vec<u8> {
        let mut q = DisseminationQueue::new(3);
        q.enqueue(membership_update(target, state, incarnation), 5);
        q.pack_piggyback(10)
    }

    #[test]
    fn a_flood_of_stale_suspect_gossip_does_not_run_self_incarnation_away() {
        // §7.1, the load-bearing `>=` bound stated as a *property over a flood*: a
        // victim pelted with many Suspect-about-self updates all at the same stale
        // incarnation refutes exactly once (to inc 1) and ignores the rest. If the
        // gate were `>` or compared against the original incarnation, the count would
        // climb with the flood — the unbounded refute storm the spec calls out.
        let mut swim = SwimNode::new(id(7), SwimConfig::default(), Instant::now());
        assert_eq!(swim.members().self_incarnation(), 0);

        // 50 stale Suspect-about-self updates, all at incarnation 0.
        for seq in 0..50u64 {
            swim.handle_ping(id(1), seq, &flood_piggyback(id(7), MemberState::Suspect, 0));
        }
        assert_eq!(
            swim.members().self_incarnation(),
            1,
            "a flood at one stale incarnation must refute exactly once (storm bound)"
        );

        // Now a genuinely fresh accusation at the *current* incarnation must still
        // get through — the bound suppresses stale gossip, not legitimate news.
        swim.handle_ping(id(1), 99, &flood_piggyback(id(7), MemberState::Suspect, 1));
        assert_eq!(
            swim.members().self_incarnation(),
            2,
            "an accusation at the current incarnation must still refute"
        );
    }
}
