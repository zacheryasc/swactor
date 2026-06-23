//! Behavioral tests for the directory **read path** (`DIRECTORY.md` §5): routing an
//! application message to an actor known only by its `ActorAddress`.
//!
//! An app actor on node A does `ctx.send(target, msg)` for a `target` hosted on
//! node B, knowing nothing about B. The runtime routes `target` through a
//! [`RouteViewTransport`] that resolves `target → host` from the directory's
//! converged [`RouteView`], and the frame carries `target` as its wire `dest` so
//! the receiver delivers it straight into that actor's mailbox.
//!
//! The harness uses the **production** §5 machinery — [`OutboxRouteBinder`],
//! [`RouteViewTransport`], [`OutboxPeerDirectory`], and the `dest`-carrying
//! [`OutFrame`] — and a `deliver_wire` step that mirrors the driver's egress
//! (`drain_outbox`) and dest-first ingress (`pump_inbound_to_actors`): a frame
//! addressed to a node's peer-mailbox is gossip (routed by tag), anything else is
//! an app message (delivered to its `dest` actor). Routing is blind best-effort —
//! an actor absent from the view drops, like a lost packet.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};

use serde::{Deserialize, Serialize};
use swactor::Error;
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime, RuntimeConfig};
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
    rt: Arc<Runtime>,
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
            let mut rt = Runtime::new(RuntimeConfig::default())
                .with_extension(Arc::new(StdExtension::new()));
            let router = Arc::new(TransportRouter::new());
            rt.set_remote_sink(Arc::new(swactor_transport::CodecRemoteSink::new(
                codec.clone(),
                router.clone(),
            )));
            let rt = Arc::new(rt);

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

    /// Move every queued frame to its destination, mirroring the live driver:
    /// `drain_outbox` (sender side) then `pump_inbound_to_actors` (receiver side,
    /// dest-first). A frame addressed to a node's peer-mailbox is gossip and is
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
                node.rt.tick();
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
