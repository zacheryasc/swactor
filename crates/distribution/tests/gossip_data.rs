//! These tests cover data planes that use SWIM membership as a live peer set but do not ride SWIM
//! membership piggyback: registry names, node metadata, and shared standalone gossip transport.
//!
//! Behavioral/correctness guarantees:
//! - Non-membership replicated data converges independently of SWIM membership gossip.
//! - Registry data resolves conflicts deterministically and tombstones deleted names.
//! - Metadata resolves conflicts deterministically and publishes relay/name state for egress.
//! - Gossip services use SWIM membership only as the live peer set, not as their data channel.
//! - Registry, metadata, and directory gossip can coexist on one actor codec/transport without
//!   message cross-talk.
//! - Stable replicated data stops producing redundant gossip after convergence.

mod registry_crdt {
    //! Registry CRDT correctness: last-writer-wins conflict order and tombstone garbage collection.

    use distribution::registry::{ClusterRegistry, RegistryConfig, RegistryEntry};
    use distribution::types::NodeId;
    use swactor::actor::ActorAddress;

    // ─── LWW conflict — higher timestamp wins ──────────────────────────────────

    #[test]
    fn lww_conflict_higher_timestamp_wins() {
        let mut reg = ClusterRegistry::new(RegistryConfig::default());
        let addr_old = ActorAddress::new_random();
        let addr_new = ActorAddress::new_random();
        let node_id = NodeId([1; 32]);

        let old_entry = RegistryEntry {
            name: "svc".into(),
            actor_addr: addr_old,
            node_id,
            timestamp: 1,
            generation: 1,
            tombstone: false,
        };
        let new_entry = RegistryEntry {
            name: "svc".into(),
            actor_addr: addr_new,
            node_id,
            timestamp: 5,
            generation: 2,
            tombstone: false,
        };

        // Merge in either order — newer timestamp wins.
        reg.merge(new_entry.clone());
        reg.merge(old_entry.clone());

        assert_eq!(reg.resolve("svc"), Some((addr_new, node_id)));
    }

    #[test]
    fn recovered_authoritative_registration_wins_over_older_disseminated_clock() {
        let mut restarted = ClusterRegistry::new(RegistryConfig::default());
        let old_address = ActorAddress::new_random();
        let recovered_address = ActorAddress::new_random();
        let node_id = NodeId([7; 32]);

        restarted.merge(RegistryEntry {
            name: "data-directory".into(),
            actor_addr: old_address,
            node_id,
            timestamp: 10_000,
            generation: 7,
            tombstone: false,
        });
        restarted.register_at(
            "data-directory".into(),
            recovered_address,
            node_id,
            1_u64 << 63,
            2,
        );

        assert_eq!(
            restarted.resolve("data-directory"),
            Some((recovered_address, node_id))
        );
    }

    // ─── LWW tiebreak — generation then node_id ────────────────────────────────

    #[test]
    fn lww_tiebreak_generation_then_node_id() {
        let mut reg = ClusterRegistry::new(RegistryConfig::default());

        let addr_a = ActorAddress::new_random();
        let addr_b = ActorAddress::new_random();
        let node_low = NodeId([0; 32]);
        let node_high = NodeId([255; 32]);

        // Same timestamp, same generation — node_id breaks the tie.
        let entry_low = RegistryEntry {
            name: "x".into(),
            actor_addr: addr_a,
            node_id: node_low,
            timestamp: 10,
            generation: 1,
            tombstone: false,
        };
        let entry_high = RegistryEntry {
            name: "x".into(),
            actor_addr: addr_b,
            node_id: node_high,
            timestamp: 10,
            generation: 1,
            tombstone: false,
        };

        reg.merge(entry_low);
        reg.merge(entry_high);

        // Higher node_id wins.
        assert_eq!(reg.resolve("x"), Some((addr_b, node_high)));

        // And same-timestamp, different-generation: higher generation wins.
        let mut reg2 = ClusterRegistry::new(RegistryConfig::default());
        let entry_gen1 = RegistryEntry {
            name: "y".into(),
            actor_addr: addr_a,
            node_id: node_low,
            timestamp: 10,
            generation: 1,
            tombstone: false,
        };
        let entry_gen2 = RegistryEntry {
            name: "y".into(),
            actor_addr: addr_b,
            node_id: node_low,
            timestamp: 10,
            generation: 2,
            tombstone: false,
        };
        reg2.merge(entry_gen1);
        reg2.merge(entry_gen2);
        assert_eq!(reg2.resolve("y"), Some((addr_b, node_low)));
    }

    // ─── Tombstone GC removes old tombstones ───────────────────────────────────

    #[test]
    fn tombstone_gc_removes_old_tombstones() {
        let mut reg = ClusterRegistry::new(RegistryConfig {
            tombstone_ttl: 10,
            gc_interval: 1,
            ..RegistryConfig::default()
        });

        let actor = ActorAddress::new_random();
        let node_id = NodeId([1; 32]);

        reg.register("gc-me".into(), actor, node_id, 1);
        reg.unregister("gc-me", node_id, 1);

        // Tombstone exists.
        assert_eq!(reg.resolve("gc-me"), None);
        assert_eq!(reg.tombstone_count(), 1);

        // Advance the clock past TTL by registering enough other things.
        for i in 0..15 {
            let a = ActorAddress::new_random();
            reg.register(format!("filler-{i}"), a, node_id, 1);
        }

        // Need to drain dissemination for "gc-me" tombstone so GC can remove it.
        for _ in 0..20 {
            reg.take_pending(100);
        }

        // Now run GC.
        reg.gc_tick();

        // The tombstone should be gone.
        assert_eq!(
            reg.tombstone_count(),
            0,
            "tombstone should be GC'd after TTL"
        );
    }
}

mod node_metadata_engine {
    //! Node metadata engine correctness: generation conflict order, local versioning, pending
    //! coalescing, and dead-node removal.

    use distribution::node_metadata::{NodeMetadataDisseminator, NodeMetadataEntry};
    use distribution::types::NodeId;

    fn id(byte: u8) -> NodeId {
        NodeId([byte; 32])
    }

    fn entry(node_id: NodeId, relay: &str, name: &str, generation: u64) -> NodeMetadataEntry {
        NodeMetadataEntry {
            node_id,
            relay_url: Some(relay.into()),
            node_name: Some(name.into()),
            generation,
        }
    }

    #[test]
    fn higher_generation_metadata_wins_and_stale_metadata_is_ignored() {
        // Correctness: metadata is a per-node replicated value where generation is the
        // only conflict clock. Older gossip cannot erase newer relay/name state.
        let mut metadata = NodeMetadataDisseminator::new(3);
        let peer = id(1);

        metadata.apply_incoming(vec![entry(peer, "relay://new", "node-new", 2)], 4);
        metadata.apply_incoming(vec![entry(peer, "relay://old", "node-old", 1)], 4);

        assert_eq!(metadata.relay_url(&peer), Some("relay://new"));
        assert_eq!(metadata.node_name(&peer), Some("node-new"));
        assert_eq!(metadata.peer_version(&peer), Some(2));
    }

    #[test]
    fn local_metadata_changes_increment_version_and_enqueue_once_per_node() {
        // Correctness: local metadata publishes a monotonically increasing generation,
        // and pending dissemination coalesces to the latest value for the node.
        let mut metadata = NodeMetadataDisseminator::new(3);
        let local = id(2);

        metadata.set_local(local, Some("relay://one".into()), Some("one".into()), 4);
        metadata.set_local(local, Some("relay://two".into()), Some("two".into()), 4);

        assert_eq!(metadata.local_version(), 2);
        assert_eq!(metadata.relay_url(&local), Some("relay://two"));
        assert_eq!(metadata.node_name(&local), Some("two"));

        let pending = metadata.take_pending(10);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].generation, 2);
        assert_eq!(pending[0].relay_url.as_deref(), Some("relay://two"));
    }

    #[test]
    fn removing_a_dead_node_clears_metadata_and_pending_gossip() {
        // Correctness: metadata for a dead peer must not keep resolving locally or leak
        // through future gossip after membership says the peer is gone.
        let mut metadata = NodeMetadataDisseminator::new(3);
        let peer = id(3);

        metadata.apply_incoming(vec![entry(peer, "relay://peer", "peer", 2)], 4);
        assert_eq!(metadata.relay_url(&peer), Some("relay://peer"));

        metadata.remove_node(&peer);

        assert_eq!(metadata.relay_url(&peer), None);
        assert_eq!(metadata.node_name(&peer), None);
        assert!(metadata.take_pending(10).is_empty());
    }
}

mod standalone_gossip_transport {
    //! Actorized registry/metadata/directory gossip over one codec and transport, proving
    //! standalone frames converge without piggybacking on SWIM.

    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    use swactor::Error;
    use swactor::actor::ActorAddress;
    use swactor::runtime::{Inbox, Runtime, RuntimeConfig, RuntimeParts};
    use swactor::std::StdExtension;
    use swactor_engine::{Engine, SteppingBackend};
    use swactor_transport::{CodecRegistry, Transport, TransportRouter, WireEnvelope};

    use distribution::crypto::{Keypair, KeypairExt};
    use distribution::directory_actor::{DirectoryActor, DirectoryIn, Located};
    use distribution::messages::actor_codec_registry;
    use distribution::node_metadata_actor::{MetadataActor, MetadataIn, RelayInfo};
    use distribution::registry::RegistryConfig;
    use distribution::registry_actor::{NameResolved, RegistryActor, RegistryIn};
    use distribution::swim::actor::{MembershipChanged, SharedPeerDirectory};
    use distribution::transport_bridge::{NoopRouteBinder, RelayMirror, RouteView, peer_addr};
    use distribution::types::{MemberState, NodeId};

    /// Carries an encoded frame into the destination runtime and performs the
    /// production ingress — decode, then `deliver_raw` to the local actor that owns
    /// the frame's `type_tag` (the tag→actor routing the real driver does).
    struct Link {
        dst_rt: Runtime,
        routes: HashMap<String, ActorAddress>,
        codec: Arc<CodecRegistry>,
    }

    impl Transport for Link {
        fn send(&self, wire: WireEnvelope) -> Result<(), Error> {
            let addr = *self
                .routes
                .get(&wire.type_tag)
                .ok_or_else(|| Error::from(format!("no local actor for tag {}", wire.type_tag)))?;
            let msg = self.codec.decode(&wire.type_tag, &wire.payload)?;
            self.dst_rt.deliver_raw(addr, msg)
        }
    }

    /// One node: a runtime hosting a RegistryActor + MetadataActor + DirectoryActor,
    /// plus the shared state needed to wire it into a mesh.
    struct Node {
        rt: Runtime,
        _engine: Engine,
        backend: SteppingBackend,
        registry: ActorAddress,
        metadata: ActorAddress,
        directory: ActorAddress,
        dir: SharedPeerDirectory,
        router: Arc<TransportRouter>,
        relay_mirror: RelayMirror,
        route_view: RouteView,
    }

    struct GossipCluster {
        nodes: Vec<Node>,
        keys: Vec<Keypair>,
        ids: Vec<NodeId>,
    }

    impl GossipCluster {
        fn new(n: usize) -> Self {
            let codec = Arc::new(actor_codec_registry());
            let keys: Vec<Keypair> = (0..n).map(|_| Keypair::generate()).collect();
            let ids: Vec<NodeId> = keys.iter().map(|k| k.node_id()).collect();

            // Phase 1: per-node runtime + actors.
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
                let backend = SteppingBackend::new();
                let engine =
                    Engine::new(parts, backend.clone()).expect("create stepping actor engine");

                let dir = SharedPeerDirectory::new();
                let relay_mirror: RelayMirror = Arc::new(RwLock::new(HashMap::new()));
                let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
                let registry = rt
                    .spawn(RegistryActor::new(
                        nid,
                        RegistryConfig::default(),
                        Arc::new(dir.clone()),
                    ))
                    .expect("spawn RegistryActor");
                let metadata = rt
                    .spawn(MetadataActor::new(
                        nid,
                        3,
                        Arc::new(dir.clone()),
                        relay_mirror.clone(),
                    ))
                    .expect("spawn MetadataActor");
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
                    _engine: engine,
                    backend,
                    registry,
                    metadata,
                    directory,
                    dir,
                    router,
                    relay_mirror,
                    route_view,
                });
            }

            // Phase 2: mesh — bind every peer's NodeId to its synthetic address and
            // route that address through a Link that tag-dispatches into the peer's
            // registry/metadata/directory actors. Also tell each actor the others are
            // Alive so cluster_size and the gossip fan-out set are populated.
            for i in 0..n {
                for j in 0..n {
                    if i == j {
                        continue;
                    }
                    let syn = peer_addr(ids[j]);
                    nodes[i].dir.bind(ids[j], syn, 0);
                    let mut routes = HashMap::new();
                    routes.insert(
                        "swactor_dist::RegistryGossip".to_string(),
                        nodes[j].registry,
                    );
                    routes.insert(
                        "swactor_dist::MetadataGossip".to_string(),
                        nodes[j].metadata,
                    );
                    routes.insert(
                        "swactor_dist::DirectoryGossip".to_string(),
                        nodes[j].directory,
                    );
                    nodes[i].router.add_route(
                        syn,
                        Arc::new(Link {
                            dst_rt: nodes[j].rt.clone(),
                            routes,
                            codec: codec.clone(),
                        }),
                    );
                    let alive = MembershipChanged {
                        node_id: ids[j],
                        state: MemberState::Alive,
                        incarnation: 1,
                    };
                    nodes[i]
                        .rt
                        .send_to(nodes[i].registry, RegistryIn::Membership(alive.clone()))
                        .unwrap();
                    nodes[i]
                        .rt
                        .send_to(nodes[i].metadata, MetadataIn::Membership(alive.clone()))
                        .unwrap();
                    nodes[i]
                        .rt
                        .send_to(nodes[i].directory, DirectoryIn::Membership(alive))
                        .unwrap();
                }
            }

            let c = GossipCluster { nodes, keys, ids };
            c.pump(4); // settle membership
            c
        }

        fn pump(&self, k: usize) {
            for _ in 0..k {
                for node in &self.nodes {
                    node.backend.step();
                }
            }
        }

        /// One dissemination round: tick the gossip clocks, then settle deliveries.
        fn round(&self) {
            for node in &self.nodes {
                let _ = node.rt.send_to(node.registry, RegistryIn::Tick);
                let _ = node.rt.send_to(node.metadata, MetadataIn::Tick);
                let _ = node.rt.send_to(node.directory, DirectoryIn::Tick);
            }
            self.pump(6);
        }

        fn run_until<F: Fn(&GossipCluster) -> bool>(&self, cap: usize, cond: F) -> bool {
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

        /// Resolve `name` on node `observer` (a local request/reply round).
        fn resolve_name(&self, observer: usize, name: &str) -> Option<(ActorAddress, NodeId)> {
            let inbox: Inbox<NameResolved> = self.nodes[observer].rt.new_inbox().unwrap();
            self.nodes[observer]
                .rt
                .send_to(
                    self.nodes[observer].registry,
                    RegistryIn::ResolveName {
                        name: name.to_string(),
                        reply: *inbox.addr(),
                    },
                )
                .unwrap();
            self.nodes[observer].backend.step();
            inbox.try_recv().and_then(|r| r.binding)
        }

        /// Look up `node`'s relay URL as seen by `observer`.
        fn relay_seen(&self, observer: usize, node: NodeId) -> Option<String> {
            let inbox: Inbox<RelayInfo> = self.nodes[observer].rt.new_inbox().unwrap();
            self.nodes[observer]
                .rt
                .send_to(
                    self.nodes[observer].metadata,
                    MetadataIn::RelayLookup {
                        node,
                        reply: *inbox.addr(),
                    },
                )
                .unwrap();
            self.nodes[observer].backend.step();
            inbox.try_recv().and_then(|r| r.relay_url)
        }

        /// The host `observer` resolves `actor` to via the directory's `Resolve` reply.
        fn host_seen(&self, observer: usize, actor: ActorAddress) -> Option<NodeId> {
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
            self.nodes[observer].backend.step();
            inbox.try_recv().and_then(|located| located.host)
        }
    }

    #[test]
    fn a_registered_name_propagates_to_a_peer_over_the_transport() {
        let c = GossipCluster::new(3);
        let svc = ActorAddress([0x42; 32]);

        // Node 0 registers a name for a local actor.
        c.nodes[0]
            .rt
            .send_to(
                c.nodes[0].registry,
                RegistryIn::RegisterName {
                    name: "billing".into(),
                    actor_addr: svc,
                },
            )
            .unwrap();

        // Every other node eventually resolves it to (actor, node0) — purely via the
        // standalone RegistryGossip frames over the transport.
        let propagated = c.run_until(400, |c| {
            (1..c.ids.len()).all(|o| c.resolve_name(o, "billing") == Some((svc, c.ids[0])))
        });
        assert!(
            propagated,
            "registered name did not propagate over the transport"
        );
    }
    #[test]
    fn directed_rebind_replaces_a_stale_binding_without_gossip_rounds() {
        use distribution::messages::RegistryGossip;
        use distribution::registry::RegistryEntry;

        let c = GossipCluster::new(2);
        let acknowledgements = c.nodes[0].rt.new_inbox::<RegistryIn>().unwrap();
        let name = "swactor.data-directory".to_owned();
        let dead = ActorAddress([0xDD; 32]);
        let fresh = ActorAddress([0xF3; 32]);

        // The peer still serves the authority's pre-crash binding.
        c.nodes[1]
            .rt
            .send_to(
                c.nodes[1].registry,
                RegistryIn::Gossip(RegistryGossip {
                    entries: vec![RegistryEntry {
                        name: name.clone(),
                        actor_addr: dead,
                        node_id: c.ids[0],
                        timestamp: (1 << 63) + 4,
                        generation: 1,
                        tombstone: false,
                    }],
                    delivery: None,
                }),
            )
            .unwrap();
        c.pump(2);
        assert_eq!(c.resolve_name(1, &name), Some((dead, c.ids[0])));

        // The restarted authority registers its recovered binding and pushes
        // it directly to the persisted peer — no Tick, no gossip round, no
        // membership convergence in between.
        c.nodes[0]
            .rt
            .send_to(
                c.nodes[0].registry,
                RegistryIn::RegisterNameAt {
                    name: name.clone(),
                    actor_addr: fresh,
                    timestamp: (1 << 63) + 5,
                },
            )
            .unwrap();
        c.nodes[0].backend.step();
        c.nodes[0]
            .rt
            .send_to(
                c.nodes[0].registry,
                RegistryIn::DisseminateNameTo {
                    name,
                    peers: vec![c.ids[1]],
                    reply_to: *acknowledgements.addr(),
                },
            )
            .unwrap();
        c.pump(4);

        assert_eq!(
            c.resolve_name(1, "swactor.data-directory"),
            Some((fresh, c.ids[0])),
            "directed re-bind must replace the dead binding without gossip rounds"
        );
        assert!(matches!(
            acknowledgements.try_recv(),
            Some(RegistryIn::NameAcknowledged { entry, peer })
                if entry.name == "swactor.data-directory"
                    && entry.actor_addr == fresh
                    && entry.node_id == c.ids[0]
                    && entry.timestamp == (1 << 63) + 5
                    && entry.generation == 1
                    && !entry.tombstone
                    && peer == c.ids[1]
        ));

        // Losing an ACK must not strand recovery: an unchanged duplicate
        // publication still acknowledges the installed winner.
        c.nodes[0]
            .rt
            .send_to(
                c.nodes[0].registry,
                RegistryIn::DisseminateNameTo {
                    name: "swactor.data-directory".to_owned(),
                    peers: vec![c.ids[1]],
                    reply_to: *acknowledgements.addr(),
                },
            )
            .unwrap();
        c.pump(4);
        assert!(matches!(acknowledgements.try_recv(),
            Some(RegistryIn::NameAcknowledged { entry, peer })
                if entry.actor_addr == fresh && peer == c.ids[1]
        ));

        // A peer that has a newer winning binding must not ACK mere receipt
        // of the stale directed request.
        c.nodes[1]
            .rt
            .send_to(
                c.nodes[1].registry,
                RegistryIn::RegisterNameAt {
                    name: "swactor.data-directory".to_owned(),
                    actor_addr: ActorAddress([0xF4; 32]),
                    timestamp: (1 << 63) + 20,
                },
            )
            .unwrap();
        c.nodes[1].backend.step();
        c.nodes[0]
            .rt
            .send_to(
                c.nodes[0].registry,
                RegistryIn::DisseminateNameTo {
                    name: "swactor.data-directory".to_owned(),
                    peers: vec![c.ids[1]],
                    reply_to: *acknowledgements.addr(),
                },
            )
            .unwrap();
        c.pump(4);
        assert!(acknowledgements.try_recv().is_none());
    }

    #[test]
    fn directed_acknowledgement_rejects_superseded_generation() {
        use distribution::messages::{RegistryDelivery, RegistryGossip};

        let c = GossipCluster::new(2);
        let replies = c.nodes[0].rt.new_inbox::<RegistryIn>().unwrap();
        let name = "swactor.data-directory".to_owned();
        let actor_addr = ActorAddress([0xF3; 32]);
        for timestamp in [100, 101] {
            c.nodes[0]
                .rt
                .send_to(
                    c.nodes[0].registry,
                    RegistryIn::RegisterNameAt {
                        name: name.clone(),
                        actor_addr,
                        timestamp,
                    },
                )
                .unwrap();
        }
        c.nodes[0].backend.step();
        // Observe the actual publication; the registry advances its logical
        // clock while merging, so requested timestamps are not clock snapshots.
        c.nodes[0].dir.bind(c.ids[1], *replies.addr(), 1);
        c.nodes[0]
            .rt
            .send_to(c.nodes[0].registry, RegistryIn::SyncTo { peer: c.ids[1] })
            .unwrap();
        c.nodes[0].backend.step();
        let Some(RegistryIn::Gossip(publication)) = replies.try_recv() else {
            panic!("registry did not publish its current binding");
        };
        let current = publication
            .entries
            .into_iter()
            .find(|entry| entry.name == name)
            .unwrap();
        let acknowledge = |entry| {
            c.nodes[0]
                .rt
                .send_to(
                    c.nodes[0].registry,
                    RegistryIn::Gossip(RegistryGossip {
                        entries: vec![entry],
                        delivery: Some(RegistryDelivery::Acknowledged {
                            reply_to: *replies.addr(),
                            peer: c.ids[1],
                        }),
                    }),
                )
                .unwrap();
            c.nodes[0].backend.step();
        };
        let mut stale = current.clone();
        stale.generation -= 1;
        acknowledge(stale);
        assert!(
            replies.try_recv().is_none(),
            "same binding is not the same generation"
        );
        let mut stale = current.clone();
        stale.timestamp -= 1;
        acknowledge(stale);
        assert!(
            replies.try_recv().is_none(),
            "stale publication cannot finish recovery"
        );
        let mut future = current.clone();
        future.timestamp += 1;
        acknowledge(future);
        assert!(
            replies.try_recv().is_none(),
            "ACKs cannot publish unseen future entries"
        );
        acknowledge(current.clone());
        assert!(matches!(replies.try_recv(),
            Some(RegistryIn::NameAcknowledged { entry, peer })
                if entry == current && peer == c.ids[1]
        ));
    }

    #[test]
    fn a_relay_url_propagates_to_a_peer_over_the_transport() {
        let c = GossipCluster::new(3);

        // Node 0 announces its relay URL.
        c.nodes[0]
            .rt
            .send_to(
                c.nodes[0].metadata,
                MetadataIn::SetRelayUrl {
                    url: Some("http://relay.example:3340/".into()),
                },
            )
            .unwrap();

        let propagated = c.run_until(400, |c| {
            (1..c.ids.len())
                .all(|o| c.relay_seen(o, c.ids[0]).as_deref() == Some("http://relay.example:3340/"))
        });
        assert!(propagated, "relay URL did not propagate over the transport");

        // And the MetadataActor mirrored it for network egress to read synchronously.
        let mirror = c.nodes[1].relay_mirror.read().unwrap();
        assert_eq!(
            mirror.get(&c.ids[0]).map(String::as_str),
            Some("http://relay.example:3340/"),
            "relay read-mirror must reflect the learned relay for the dial path"
        );
    }

    #[test]
    fn all_three_gossip_protocols_coexist_on_one_transport() {
        // Coexistence: a name (registry), a relay URL (metadata), and an actor→host
        // claim (directory) registered on node 0 all converge to every peer over the
        // *same* codec registry, transport router, and tag→actor ingress table — no
        // frame type clobbers another.
        let c = GossipCluster::new(3);
        let svc = ActorAddress([0x42; 32]);
        let app_actor = ActorAddress([0x99; 32]);

        c.nodes[0]
            .rt
            .send_to(
                c.nodes[0].registry,
                RegistryIn::RegisterName {
                    name: "billing".into(),
                    actor_addr: svc,
                },
            )
            .unwrap();
        c.nodes[0]
            .rt
            .send_to(
                c.nodes[0].metadata,
                MetadataIn::SetRelayUrl {
                    url: Some("http://relay.example:3340/".into()),
                },
            )
            .unwrap();
        // The directory claim is signed by node 0's key, so its host is c.ids[0].
        let claim = c.keys[0].sign_directory_entry(app_actor, 1);
        c.nodes[0]
            .rt
            .send_to(c.nodes[0].directory, DirectoryIn::Register(claim))
            .unwrap();

        let all_converged = c.run_until(400, |c| {
            (1..c.ids.len()).all(|o| {
                c.resolve_name(o, "billing") == Some((svc, c.ids[0]))
                    && c.relay_seen(o, c.ids[0]).as_deref() == Some("http://relay.example:3340/")
                    && c.host_seen(o, app_actor) == Some(c.ids[0])
                    && c.nodes[o].route_view.read().unwrap().get(&app_actor) == Some(&c.ids[0])
            })
        });
        assert!(
            all_converged,
            "registry, metadata, and directory gossip did not all converge on one transport"
        );
    }
}
