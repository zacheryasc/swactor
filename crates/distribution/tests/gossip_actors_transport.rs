//! Transport-seam behavioral tests for the standalone gossip actors
//! (`RegistryActor`, `MetadataActor`, `DirectoryActor`).
//!
//! These prove the Stage-3 cutover: registry names, node metadata, and actor→host
//! directory claims no longer ride the SWIM piggyback — they travel as their own
//! `RegistryGossip` / `MetadataGossip` / `DirectoryGossip` frames, disseminated on
//! the actors' own `Tick`, and converge across *separate runtimes* over the real
//! codec/TransportRouter/deliver_raw path. The directory tests additionally prove
//! **coexistence**: all three frame types share one codec registry, one transport
//! router, and one tag→actor ingress table without clobbering each other. We assert
//! only the observable effect (a peer resolves the name / learns the relay / routes
//! the actor), never queue internals, and pin eventuality by converge-or-timeout.
//!
//! Node identities are real keypairs: a directory claim is signed over
//! `(actor, host, generation)` and verified against its host on merge, so synthetic
//! ids could not produce a verifiable claim.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use swactor::Error;
use swactor::actor::ActorAddress;
use swactor::runtime::{Inbox, Runtime, RuntimeConfig};
use swactor::std::StdExtension;
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
    dst_rt: Arc<Runtime>,
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
    rt: Arc<Runtime>,
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
            let mut rt = Runtime::new(RuntimeConfig::default())
                .with_extension(Arc::new(StdExtension::new()));
            let router = Arc::new(TransportRouter::new());
            rt.set_remote_sink(Arc::new(swactor_transport::CodecRemoteSink::new(
                codec.clone(),
                router.clone(),
            )));
            let rt = Arc::new(rt);

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
                node.rt.tick();
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
        self.nodes[observer].rt.tick();
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
        self.nodes[observer].rt.tick();
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
        self.nodes[observer].rt.tick();
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
