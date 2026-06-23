//! Bridge between a real transport (iroh) and the swactor runtime for the
//! actorized distribution protocol.
//!
//! # Egress
//! An actor reaches a peer by `ctx.send(peer_addr, SwimIn::…)`, where `peer_addr`
//! is the peer's *synthetic* mailbox address (see [`peer_addr`]). That address is
//! never local, so the runtime encodes the message via the actor
//! [`CodecRegistry`](swactor_transport::CodecRegistry) and routes it through the
//! [`TransportRouter`] to an [`IrohPeerTransport`], which simply **enqueues** the
//! framed bytes on a shared [`Outbox`]. The driver loop drains the outbox and
//! performs the actual iroh write on its own thread (it owns the connection
//! cache). Worker threads therefore never block on I/O, and the connection
//! lifecycle stays single-threaded — exactly as before the actorization.
//!
//! # Ingress
//! The driver decodes each received frame via the same `CodecRegistry` and
//! `deliver_raw`s it straight into the target actor's mailbox; that half lives in
//! the driver (it owns the iroh readers), not here.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, RwLock};

use swactor::Error;
use swactor::actor::ActorAddress;
use swactor_transport::{Transport, TransportRouter, WireEnvelope};

use crate::swim::actor::PeerDirectory;
use crate::types::NodeId;

/// Lock-free-ish shared view of every known peer's relay URL, published by the
/// `MetadataActor` and read synchronously by the iroh egress when it dials (the
/// dial path can't block to `ask` an actor). A snapshot mirror, not a channel.
pub type RelayMirror = Arc<RwLock<HashMap<NodeId, String>>>;

/// Lock-free-ish shared view of every known actor's host, published by the
/// `DirectoryActor` and read synchronously by the egress when it routes an
/// application message to an actor it knows only by address (`DIRECTORY.md` §5).
/// The same read-mirror discipline as [`RelayMirror`]: a single-writer snapshot
/// the routing hot path reads without blocking to `ask` an actor. Best-effort —
/// an actor absent from the mirror simply isn't routable yet (the message drops,
/// like a lost packet).
pub type RouteView = Arc<RwLock<HashMap<ActorAddress, NodeId>>>;

/// The deterministic, reversible `NodeId → ActorAddress` mapping for a *remote*
/// peer's mailbox: the address is the node id's raw bytes.
///
/// This is never a locally-spawned actor's address (those are `new_random()`),
/// so `ctx.send(peer_addr(n), …)` always takes the transport egress path; and it
/// inverts trivially (`NodeId(addr.0)`) so a write failure maps back to the peer.
pub fn peer_addr(node: NodeId) -> ActorAddress {
    ActorAddress(node.0)
}

/// A single outbound frame the actors produced, awaiting an iroh write by the
/// driver loop. `type_tag` + `payload` are already encoded by the runtime's
/// `CodecRegistry`; the driver just frames and writes them to `to`'s connection.
///
/// `dest` is the *destination actor address* (`DIRECTORY.md` §5). For gossip to a
/// well-known protocol actor it is `peer_addr(to)` (the receiver routes it by tag);
/// for an application message routed by the directory it is the target actor's own
/// address, which the receiver delivers straight into that actor's mailbox.
#[derive(Debug, Clone)]
pub struct OutFrame {
    pub to: NodeId,
    pub dest: ActorAddress,
    pub type_tag: String,
    pub payload: Vec<u8>,
}

/// Shared queue of outbound frames, written by worker threads (via
/// [`IrohPeerTransport`]) and drained by the single driver loop.
pub type Outbox = Arc<Mutex<Vec<OutFrame>>>;

/// Per-peer egress transport. The runtime hands it an already-encoded
/// [`WireEnvelope`] bound for this peer; it just records the frame on the shared
/// outbox. It touches no iroh state, so it is trivially `Send + Sync` (required
/// by the [`Transport`] supertrait) and never blocks the calling worker.
struct IrohPeerTransport {
    node_id: NodeId,
    outbox: Outbox,
}

impl Transport for IrohPeerTransport {
    fn send(&self, envelope: WireEnvelope) -> Result<(), Error> {
        self.outbox.lock().expect("outbox poisoned").push(OutFrame {
            to: self.node_id,
            dest: envelope.dest,
            type_tag: envelope.type_tag,
            payload: envelope.payload,
        });
        Ok(())
    }
}

/// Production [`PeerDirectory`]: resolves every `NodeId` to its synthetic mailbox
/// address ([`peer_addr`]) and, the first time it is asked for a given peer,
/// lazily registers that peer's egress route on the shared [`TransportRouter`].
///
/// So any peer an actor decides to contact becomes reachable on demand — there
/// is no separate "binder" step and no window where a known peer lacks a route.
/// Cheaply cloned (`Arc` the whole thing) and shared across every protocol actor.
pub struct IrohPeerDirectory {
    router: Arc<TransportRouter>,
    outbox: Outbox,
    bound: Mutex<HashSet<NodeId>>,
}

impl IrohPeerDirectory {
    pub fn new(router: Arc<TransportRouter>, outbox: Outbox) -> Self {
        Self {
            router,
            outbox,
            bound: Mutex::new(HashSet::new()),
        }
    }
}

impl PeerDirectory for IrohPeerDirectory {
    fn resolve(&self, node: &NodeId) -> Option<ActorAddress> {
        let addr = peer_addr(*node);
        // Register the egress route once, on first contact.
        if self.bound.lock().expect("bound set poisoned").insert(*node) {
            self.router.add_route(
                addr,
                Arc::new(IrohPeerTransport {
                    node_id: *node,
                    outbox: self.outbox.clone(),
                }),
            );
        }
        Some(addr)
    }
}

/// Egress transport for an **application** message addressed to an actor by its
/// own address (`DIRECTORY.md` §5). Where [`IrohPeerTransport`] knows its peer at
/// construction, this one discovers the host at send time from the directory's
/// [`RouteView`]: it resolves `envelope.dest → host` and enqueues the frame for
/// that host. A `dest` absent from the view is dropped, like a lost packet — the
/// blind best-effort routing contract. One shared instance backs every routed
/// actor address (see [`IrohRouteBinder`]); re-checking the view on each send
/// means a stale registered route is harmless (it simply drops on a miss).
pub struct RouteViewTransport {
    route_view: RouteView,
    outbox: Outbox,
}

impl RouteViewTransport {
    pub fn new(route_view: RouteView, outbox: Outbox) -> Self {
        Self { route_view, outbox }
    }
}

impl Transport for RouteViewTransport {
    fn send(&self, envelope: WireEnvelope) -> Result<(), Error> {
        let host = self
            .route_view
            .read()
            .expect("route view poisoned")
            .get(&envelope.dest)
            .copied();
        if let Some(node) = host {
            self.outbox.lock().expect("outbox poisoned").push(OutFrame {
                to: node,
                dest: envelope.dest,
                type_tag: envelope.type_tag,
                payload: envelope.payload,
            });
        }
        // None => no known host yet; drop best-effort (DIRECTORY.md §5).
        Ok(())
    }
}

/// Makes a remotely-hosted actor address reachable through the runtime's
/// [`TransportRouter`]. swactor routes egress per-destination-address, so before
/// `ctx.send(actor, …)` to a remote actor can succeed there must be a registered
/// route for `actor`; this seam registers it lazily.
///
/// The [`DirectoryActor`](crate::directory_actor::DirectoryActor) calls
/// [`ensure_routable`](RouteBinder::ensure_routable) for every actor it learns is
/// hosted on a peer, mirroring how [`IrohPeerDirectory`] lazily binds a peer's
/// egress on first contact.
pub trait RouteBinder: Send + Sync + 'static {
    fn ensure_routable(&self, actor: ActorAddress);
}

/// Production [`RouteBinder`]: registers each remote actor address against one
/// shared [`RouteViewTransport`] on the runtime's [`TransportRouter`], once.
///
/// Because the single transport re-resolves the [`RouteView`] on every send, the
/// route never needs updating when an actor moves hosts — only registering once,
/// the first time the directory learns the actor exists elsewhere.
pub struct IrohRouteBinder {
    router: Arc<TransportRouter>,
    transport: Arc<RouteViewTransport>,
    bound: Mutex<HashSet<ActorAddress>>,
}

impl IrohRouteBinder {
    pub fn new(router: Arc<TransportRouter>, transport: Arc<RouteViewTransport>) -> Self {
        Self {
            router,
            transport,
            bound: Mutex::new(HashSet::new()),
        }
    }
}

impl RouteBinder for IrohRouteBinder {
    fn ensure_routable(&self, actor: ActorAddress) {
        if self.bound.lock().expect("bound set poisoned").insert(actor) {
            self.router.add_route(actor, self.transport.clone());
        }
    }
}

/// A no-op [`RouteBinder`] for harnesses that route frames by hand (no
/// `TransportRouter` egress to register against).
pub struct NoopRouteBinder;

impl RouteBinder for NoopRouteBinder {
    fn ensure_routable(&self, _actor: ActorAddress) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(b: u8) -> NodeId {
        NodeId([b; 32])
    }

    #[test]
    fn peer_addr_round_trips_through_node_id() {
        // The egress derives the peer mailbox from the NodeId, and a write
        // failure must be able to recover the NodeId from the frame's target —
        // so the mapping has to be exactly reversible.
        let n = id(0x5A);
        assert_eq!(NodeId(peer_addr(n).0), n);
    }

    #[test]
    fn an_encoded_frame_is_enqueued_for_the_peer_it_targets() {
        // The egress contract: a routed WireEnvelope becomes an OutFrame the
        // driver can write, preserving the target peer, wire tag, and bytes.
        let outbox: Outbox = Arc::new(Mutex::new(Vec::new()));
        let peer = id(7);
        let transport = IrohPeerTransport {
            node_id: peer,
            outbox: outbox.clone(),
        };
        transport
            .send(WireEnvelope {
                dest: peer_addr(peer),
                type_tag: "swactor_dist::Ping".into(),
                payload: vec![1, 2, 3],
            })
            .unwrap();

        let frames = outbox.lock().unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].to, peer);
        assert_eq!(frames[0].type_tag, "swactor_dist::Ping");
        assert_eq!(frames[0].payload, vec![1, 2, 3]);
    }
}
