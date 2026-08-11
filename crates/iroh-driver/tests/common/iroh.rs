//! Test helpers for iroh-based integration tests, on the **actor path**.
//!
//! Each node is an [`IrohNode`]: a real iroh [`IrohDriver`] (endpoint with
//! `RelayMode::Disabled`) bridged to a per-node swactor [`Runtime`] hosting the
//! four protocol actors — `SwimActor`, `RegistryActor`, `MetadataActor`,
//! `DirectoryActor`. Each node owns a swactor [`Engine`] that drives actor
//! progression and injects protocol ticks; iroh adapter progression is
//! engine-hosted. The driver decodes inbound frames into actor mailboxes, the
//! actors enqueue outbound frames on a shared [`Outbox`], and engine-hosted
//! writers send them to iroh.
//!
//! The synchronous `#[test]`s observe the stack through converge-or-timeout
//! polls: the `pump_until*` helpers sleep ~10ms between checks and re-read the
//! membership mirror. The engine drives core progression, protocol ticks, and
//! all iroh work in the background; the tests no longer pump any queue.
//! Membership is observed through the harness `membership_mirror` (a
//! `MemberList` filled by the [`MembershipFanout`] from SWIM's
//! `MembershipChanged` stream). The driver snapshot no longer carries members.
#![allow(dead_code)]

use std::collections::HashMap;
use std::ops::{Index, IndexMut};
use std::sync::{Arc, Mutex as StdMutex, RwLock};
use std::time::{Duration, Instant};

use iroh::{EndpointAddr, PublicKey, RelayMode};
use parking_lot::Mutex;
use swactor_engine::{Engine, TokioBackend, TokioConfig};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::config::RuntimeConfig;
use swactor::runtime::{Ctx, Runtime, RuntimeParts};
use swactor::std::StdExtension;
use swactor_transport::TransportRouter;

use iroh_driver::{IrohDriver, IrohDriverConfig};

use distribution::directory_actor::{DirectoryActor, DirectoryIn};
use distribution::messages::actor_codec_registry;
use distribution::node_metadata_actor::{MetadataActor, MetadataIn};
use distribution::peer_auth::PeerAllowList;
use distribution::registry_actor::{RegistryActor, RegistryIn};
use distribution::swim::actor::{MembershipChanged, SwimActor, SwimIn};
use distribution::swim::member_list::MemberList;
use distribution::transport_bridge::{
    Outbox, OutboxPeerDirectory, OutboxRouteBinder, RelayMirror, RouteView, RouteViewTransport,
};
use distribution::types::{MemberState, NodeId};

use super::test_config;


// ── Membership fanout (copied verbatim from main.rs) ────────────────────────
// Adapts the SwimActor's `MembershipChanged` stream (its sole observable) into
// the registry/metadata/directory actors' `Membership` control messages, and
// folds it into a mirror the test reads. The mirror's sentinel self-id
// (`NodeId([0xFF; 32])`) means every real node is stored (a MemberList never
// stores self).
struct MembershipFanout {
    registry: ActorAddress,
    metadata: ActorAddress,
    directory: ActorAddress,
    mirror: Arc<Mutex<MemberList>>,
}

impl ActorInterface for MembershipFanout {
    type Incoming = MembershipChanged;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, m: Self::Incoming) {
        self.mirror.lock().apply(m.node_id, m.state, m.incarnation);
        let _ = ctx.send(self.registry, RegistryIn::Membership(m.clone()));
        let _ = ctx.send(self.metadata, MetadataIn::Membership(m.clone()));
        let _ = ctx.send(self.directory, DirectoryIn::Membership(m));
    }
}

// ── Per-node actor stack ────────────────────────────────────────────────────

/// One node: a real iroh [`IrohDriver`] bridged to a swactor [`Runtime`] hosting
/// the four protocol actors. Owns everything that must stay alive and be pumped.
pub struct IrohNode {
    pub driver: IrohDriver,
    rt: Runtime,
    outbox: Outbox,
    swim_addr: ActorAddress,
    registry_addr: ActorAddress,
    metadata_addr: ActorAddress,
    directory_addr: ActorAddress,
    membership_mirror: Arc<Mutex<MemberList>>,
    relay_mirror: RelayMirror,
    route_view: RouteView,
    /// The engine that owns this node's Tokio substrate and drives the core
    /// runtime. Declared last so it drops after the driver on teardown.
    _engine: Engine,
}

impl IrohNode {
    /// Build a node from a driver config: build outbox/relay_mirror/route_view;
    /// `OutboxPeerDirectory::new(router, outbox)`; spawn the 4 actors;
    /// `RouteViewTransport` + `OutboxRouteBinder`; `MembershipFanout` + `Subscribe`;
    /// the `routes` tag table; `enable_actor_bridge`).
    fn from_config(config: IrohDriverConfig) -> Self {
        // Per-node swactor runtime + codec + transport router. The runtime is
        // created before the driver so the engine can own it; the driver needs
        // the engine handle, and actors need the driver's node_id.
        let parts = RuntimeParts::new(RuntimeConfig::default())
            .with_extension(Arc::new(StdExtension::new()));
        let rt = parts.runtime().clone();
        let actor_codec = Arc::new(actor_codec_registry());
        let transport_router = Arc::new(TransportRouter::new());
        rt.set_remote_sink(Arc::new(swactor_transport::CodecRemoteSink::new(
            Arc::clone(&actor_codec),
            Arc::clone(&transport_router),
        )));

        // The engine owns the runtime workers (drives actor progression) and the
        // Tokio substrate (schedules all iroh background work). The cloned
        // `Runtime` handle remains available for actor spawning and sends.
        let engine = Engine::new(
            parts,
            TokioBackend::new(TokioConfig::default()).expect("build test tokio backend"),
        )
        .expect("build test engine");

        let mut driver = IrohDriver::with_engine(engine.handle(), config)
            .expect("failed to create iroh driver");
        let node_id = driver.node_id();

        // The node's distribution config (SWIM/registry/metadata params).
        let node_config = test_config();
        let swim_config = node_config.swim.clone();
        let registry_config = node_config.registry.clone();
        let metadata_lambda = node_config.metadata_lambda;

        // Shared egress state.
        let outbox: Outbox = Arc::new(StdMutex::new(Vec::new()));
        let relay_mirror: RelayMirror = Arc::new(RwLock::new(HashMap::new()));
        let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
        let peer_directory = Arc::new(OutboxPeerDirectory::new(
            Arc::clone(&transport_router),
            Arc::clone(&outbox),
        ));

        // The four protocol actors.
        let swim_addr = rt
            .spawn(SwimActor::new(
                node_id,
                swim_config,
                Instant::now(),
                peer_directory.clone(),
            ))
            .expect("spawn SwimActor");
        let registry_addr = rt
            .spawn(RegistryActor::new(
                node_id,
                registry_config,
                peer_directory.clone(),
            ))
            .expect("spawn RegistryActor");
        let metadata_addr = rt
            .spawn(MetadataActor::new(
                node_id,
                metadata_lambda,
                peer_directory.clone(),
                Arc::clone(&relay_mirror),
            ))
            .expect("spawn MetadataActor");
        let route_view_transport = Arc::new(RouteViewTransport::new(
            Arc::clone(&route_view),
            Arc::clone(&outbox),
        ));
        let route_binder = Arc::new(OutboxRouteBinder::new(
            Arc::clone(&transport_router),
            Arc::clone(&route_view_transport),
        ));
        let directory_addr = rt
            .spawn(DirectoryActor::new(
                node_id,
                peer_directory.clone(),
                Arc::clone(&route_view),
                route_binder,
            ))
            .expect("spawn DirectoryActor");

        // Fan SWIM's MembershipChanged stream into the other actors + the mirror.
        let membership_mirror = Arc::new(Mutex::new(MemberList::new(NodeId([0xFF; 32]))));
        let fanout_addr = rt
            .spawn(MembershipFanout {
                registry: registry_addr,
                metadata: metadata_addr,
                directory: directory_addr,
                mirror: Arc::clone(&membership_mirror),
            })
            .expect("spawn MembershipFanout");
        rt.send_to(
            swim_addr,
            SwimIn::Subscribe {
                observer: fanout_addr,
            },
        )
        .expect("subscribe membership fanout");

        // Ingress routing table: which local actor owns each inbound wire tag.
        let mut routes: HashMap<String, ActorAddress> = HashMap::new();
        for tag in [
            "swactor_dist::Ping",
            "swactor_dist::Ack",
            "swactor_dist::PingReq",
            "swactor_dist::IndirectAck",
            "swactor_dist::JoinRequest",
            "swactor_dist::JoinResponse",
        ] {
            routes.insert(tag.to_string(), swim_addr);
        }
        routes.insert("swactor_dist::RegistryGossip".to_string(), registry_addr);
        routes.insert("swactor_dist::MetadataGossip".to_string(), metadata_addr);
        routes.insert("swactor_dist::DirectoryGossip".to_string(), directory_addr);
        driver.enable_actor_bridge(
            rt.clone(),
            Arc::clone(&actor_codec),
            routes,
            swim_addr,
            Arc::clone(&relay_mirror),
            Arc::clone(&route_view),
            Arc::clone(&outbox),
        );
        // Engine-hosted adapter pump: drains ingress/egress/datastream/edge on
        // a timer so the synchronous test loop no longer pumps these by hand.
        driver.install_actor_bridge_pump(Duration::from_millis(10));

        // Engine-hosted protocol tick injection + core progression. The pump
        // helpers only drain iroh queues; the engine drives actor ticks and
        // protocol injection (ENGINE_SPEC.md).
        let ticker_handle = engine.handle();
        let ticker_inner = ticker_handle.clone();
        let ticker_rt = rt.clone();
        ticker_handle.spawn(async move {
            let mut interval = ticker_inner.interval(Duration::from_millis(10));
            loop {
                (&mut interval).await;
                let now = Instant::now();
                let _ = ticker_rt.send_to(swim_addr, SwimIn::Tick { now });
                let _ = ticker_rt.send_to(registry_addr, RegistryIn::Tick);
                let _ = ticker_rt.send_to(metadata_addr, MetadataIn::Tick);
                let _ = ticker_rt.send_to(directory_addr, DirectoryIn::Tick);
            }
        });

        Self {
            driver,
            rt,
            outbox,
            swim_addr,
            registry_addr,
            metadata_addr,
            directory_addr,
            membership_mirror,
            relay_mirror,
            route_view,
            _engine: engine,
        }
    }


    // ── Passthroughs to the driver (keep consumer churn small) ──────────────

    pub fn join(&mut self, seeds: &[EndpointAddr]) {
        self.driver.join(seeds)
    }

    pub fn node_id(&self) -> NodeId {
        self.driver.node_id()
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.driver.endpoint_addr()
    }

    pub fn listen_addr(&self) -> String {
        self.driver.listen_addr()
    }

    pub fn directory_route_count(&self) -> usize {
        self.driver.directory_route_count()
    }

    pub fn shutdown(&mut self) {
        self.driver.shutdown()
    }

    /// The public key (SWIM/gossip identity) of this node.
    pub fn key(&self) -> PublicKey {
        PublicKey::from_bytes(&self.node_id().0).expect("valid node id")
    }

    /// Number of peers this node currently sees as `Alive`, derived from the
    /// membership mirror (the same source production's snapshot uses).
    pub fn alive_count(&self) -> usize {
        self.membership_mirror
            .lock()
            .all_members()
            .iter()
            .filter(|e| e.state == MemberState::Alive)
            .count()
    }
}

// ─── Single-node helpers ────────────────────────────────────────────────────

pub fn make_driver() -> IrohNode {
    IrohNode::from_config(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Disabled,
        node: test_config(),
        peer_auth: None,
        additional_alpns: vec![],
    })
}

pub fn make_driver_with_auth(auth: Arc<Mutex<PeerAllowList>>) -> IrohNode {
    IrohNode::from_config(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Disabled,
        node: test_config(),
        peer_auth: Some(auth),
        additional_alpns: vec![],
    })
}

pub fn make_driver_with_relay(relay_url: iroh::RelayUrl) -> IrohNode {
    IrohNode::from_config(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Custom(relay_url.into()),
        node: test_config(),
        peer_auth: None,
        additional_alpns: vec![],
    })
}


/// Poll until `check_fn` holds over `a` and `b` or `timeout` elapses, sleeping
/// ~10ms between checks. Progression is engine-hosted; this only waits for
/// wall-clock SWIM convergence.
pub fn pump_until_pair(
    a: &mut IrohNode,
    b: &mut IrohNode,
    timeout: Duration,
    check_fn: fn(&IrohNode, &IrohNode) -> bool,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check_fn(a, b) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Poll until `check_fn` holds over `nodes` or `timeout` elapses, sleeping
/// ~10ms between checks. Progression is engine-hosted; this only waits for
/// wall-clock SWIM convergence.
pub fn pump_until<F>(nodes: &mut [IrohNode], timeout: Duration, check_fn: F) -> bool
where
    F: Fn(&[IrohNode]) -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if check_fn(nodes) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

// ── Membership observability (reads the harness mirror, like production) ─────

/// Whether `node` sees `peer_key` in membership `state`. Membership comes from
/// the [`MembershipFanout`]-filled mirror, not the (now memberless) driver
/// snapshot.
pub fn sees_state(node: &IrohNode, peer_key: &PublicKey, state: &str) -> bool {
    let want = match state {
        "alive" => MemberState::Alive,
        "suspect" => MemberState::Suspect,
        "dead" => MemberState::Dead,
        other => panic!("unknown membership state {other:?}"),
    };
    let peer_bytes = *peer_key.as_bytes();
    node.membership_mirror
        .lock()
        .all_members()
        .iter()
        .any(|e| e.node_id.0 == peer_bytes && e.state == want)
}

/// Whether `node` sees `peer_key` as alive.
pub fn sees_alive(node: &IrohNode, peer_key: &PublicKey) -> bool {
    sees_state(node, peer_key, "alive")
}

/// Whether `node` has converged on `peer_key` being dead.
pub fn sees_dead(node: &IrohNode, peer_key: &PublicKey) -> bool {
    sees_state(node, peer_key, "dead")
}

// ─── Local relay ────────────────────────────────────────────────────────────

/// Guard that keeps the relay server alive while it exists.
pub struct RelayGuard {
    _server: iroh_relay::server::Server,
    _rt: tokio::runtime::Runtime,
}

/// Spawn a local HTTP relay server for tests. Returns the relay URL and a
/// guard that shuts the server down on drop.
pub fn spawn_test_relay() -> (iroh::RelayUrl, RelayGuard) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let server = rt
        .block_on(async {
            iroh_relay::server::Server::spawn(iroh_relay::server::ServerConfig::<(), ()> {
                relay: Some(iroh_relay::server::RelayConfig {
                    http_bind_addr: (std::net::Ipv4Addr::LOCALHOST, 0).into(),
                    tls: None,
                    limits: Default::default(),
                    key_cache_capacity: Some(256),
                    access: iroh_relay::server::AccessConfig::Everyone,
                }),
                quic: None,
                metrics_addr: None,
            })
            .await
        })
        .unwrap();
    let url = server.http_url().expect("relay has no HTTP URL");
    (
        url,
        RelayGuard {
            _server: server,
            _rt: rt,
        },
    )
}

// ─── N-node cluster ─────────────────────────────────────────────────────────

/// An N-node iroh test cluster with real QUIC endpoints, each backed by a full
/// per-node actor stack ([`IrohNode`]).
pub struct IrohTestCluster {
    nodes: Vec<IrohNode>,
}

impl IrohTestCluster {
    /// Create N disconnected nodes (no joins).
    pub fn disconnected(n: usize) -> Self {
        let nodes = (0..n).map(|_| make_driver()).collect();
        Self { nodes }
    }

    /// Create N nodes connected in a star topology through node 0.
    /// Nodes 1..N join node 0 using its full `EndpointAddr`.
    pub fn star(n: usize) -> Self {
        assert!(n >= 2, "star cluster requires at least 2 nodes");
        let mut nodes: Vec<IrohNode> = (0..n).map(|_| make_driver()).collect();

        let addr_0 = nodes[0].endpoint_addr();
        for i in 1..n {
            nodes[i].join(&[addr_0.clone()]);
        }

        Self { nodes }
    }

    /// Number of nodes in the cluster.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// The public key (SWIM/gossip identity) of node `idx`.
    pub fn key(&self, idx: usize) -> PublicKey {
        self.nodes[idx].key()
    }

    /// Poll until `check_fn` holds over the full node slice or `timeout`
    /// elapses, sleeping ~10ms between checks. Actor progression and iroh
    /// adapter work are engine-hosted, so this loop only waits for wall-clock
    /// SWIM convergence — it no longer drives nodes.
    pub fn pump_until<F>(&mut self, timeout: Duration, check_fn: F) -> bool
    where
        F: Fn(&[IrohNode]) -> bool,
    {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if check_fn(&self.nodes) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        false
    }

    /// Shut down a single node — a genuine death the survivors must detect.
    pub fn shutdown_one(&mut self, idx: usize) {
        self.nodes[idx].shutdown();
    }

    /// Shut down all nodes.
    pub fn shutdown(&mut self) {
        for n in &mut self.nodes {
            n.shutdown();
        }
    }
}

impl Index<usize> for IrohTestCluster {
    type Output = IrohNode;
    fn index(&self, idx: usize) -> &Self::Output {
        &self.nodes[idx]
    }
}

impl IndexMut<usize> for IrohTestCluster {
    fn index_mut(&mut self, idx: usize) -> &mut Self::Output {
        &mut self.nodes[idx]
    }
}
