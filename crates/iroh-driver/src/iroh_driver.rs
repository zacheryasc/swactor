//! iroh-based P2P network transport bridge for the actorized distribution
//! protocol (the SWIM / registry / metadata / directory actors).
//!
//! Uses iroh's QUIC-based peer-to-peer transport with built-in TLS, NAT
//! hole-punching, and relay server fallback.
//!
//! Runtime ownership is intentionally explicit in new call sites: prefer
//! [`IrohDriver::with_handle`] so the caller supplies the Tokio engine used for
//! iroh accepts, dials, stream I/O, retries, and shutdown. The legacy
//! [`IrohDriver::new`] convenience still performs ambient-runtime detection and
//! silently builds an owned multi-threaded Tokio runtime when no ambient handle
//! exists; see the crate README before using it.

use std::collections::{HashMap, HashSet};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayMode, SecretKey};
use tokio::runtime::Handle;

use distribution::crypto::{Keypair, KeypairExt};
use distribution::messages::*;
use distribution::node::DistributedNodeConfig;
use distribution::peer_auth::PeerAllowList;
use distribution::snapshot::DistributionNodeSnapshot;
use distribution::swim::actor::SwimIn;
use distribution::transport_bridge::{Outbox, RelayMirror, RouteView, peer_addr};
use distribution::types::NodeId;

use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor_transport::CodecRegistry;

/// ALPN protocol identifier for SWIM messages over iroh.
const ALPN: &[u8] = b"swactor/swim/1";

// ─── Config ─────────────────────────────────────────────────────────────────

/// Configuration for the iroh-based driver.
pub struct IrohDriverConfig {
    /// Secret key for the iroh endpoint.
    /// If `None`, a fresh key is generated (node gets a random identity).
    pub secret_key: Option<SecretKey>,
    /// Relay server configuration.
    /// Defaults to `RelayMode::Default` (n0 production relays).
    pub relay_mode: RelayMode,
    /// Protocol-layer configuration.
    pub node: DistributedNodeConfig,
    /// Optional peer allow-list. If provided, only allowed peers can connect.
    pub peer_auth: Option<Arc<Mutex<PeerAllowList>>>,
    /// Additional ALPNs to register beyond SWIM. Opaque to the driver.
    pub additional_alpns: Vec<Vec<u8>>,
    /// If set, start an embedded relay server on this address.
    /// Requires the `relay` feature. On success, the driver uses the embedded
    /// relay for `RelayMode::Custom`; on failure, falls back to `relay_mode`.
    #[cfg(feature = "relay")]
    pub embedded_relay_bind: Option<std::net::SocketAddr>,
    /// Public IP to advertise in the relay URL instead of the bind address.
    /// When `Some`, the relay URL uses this IP; when `None`, falls back to the
    /// bind address (which may be `0.0.0.0`).
    #[cfg(feature = "relay")]
    pub relay_public_ip: Option<std::net::IpAddr>,
}

// ─── Pending join result ────────────────────────────────────────────────────

/// Result of a background join attempt, collected during `recv()`.
struct JoinResult {
    node_id: NodeId,
    conn: Connection,
}

// ─── LAN IP Discovery ──────────────────────────────────────────────────────

/// How a peer is currently reachable, derived from iroh's `RemoteInfo`.
/// Strings on the wire (serde) rather than newtypes — iroh's own vocabulary
/// evolves and we want the format to be forgiving.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ConnType {
    Direct,
    Relay,
    Mixed,
    None,
}

/// Classify a peer's active connection type from its iroh `RemoteInfo`:
/// `Direct` (an active IP path), `Relay` (active relay path), `Mixed` (both),
/// or `None` (known peer but no active path).
pub fn conn_type_of(info: &iroh::endpoint::RemoteInfo) -> ConnType {
    let mut active_direct = false;
    let mut active_relay = false;
    for addr_info in info.addrs() {
        let is_active = format!("{:?}", addr_info.usage()).to_lowercase() == "active";
        match addr_info.addr() {
            iroh::TransportAddr::Ip(_) => {
                if is_active {
                    active_direct = true;
                }
            }
            iroh::TransportAddr::Relay(_) => {
                if is_active {
                    active_relay = true;
                }
            }
            _ => {}
        }
    }
    match (active_direct, active_relay) {
        (true, true) => ConnType::Mixed,
        (true, false) => ConnType::Direct,
        (false, true) => ConnType::Relay,
        // We've heard of the peer but no addr is in active use.
        (false, false) => ConnType::None,
    }
}

/// Discover all non-loopback LAN IP addresses on this host.
///
/// Uses UDP socket tricks to multiple broadcast destinations to find
/// addresses across different subnets. Also parses `/proc/net/if_inet6`
/// for IPv6 addresses on Linux.
pub fn discover_lan_ips() -> Vec<IpAddr> {
    let mut ips = Vec::new();
    let mut seen = std::collections::HashSet::new();

    // UDP socket trick: connect to a broadcast-ish address, read local_addr
    let targets: &[&str] = &["10.255.255.255:1", "192.168.255.255:1", "172.31.255.255:1"];
    for target in targets {
        if let Ok(sock) = std::net::UdpSocket::bind("0.0.0.0:0") {
            if sock.connect(target).is_ok() {
                if let Ok(local) = sock.local_addr() {
                    let ip = local.ip();
                    if !ip.is_loopback() && !ip.is_unspecified() && seen.insert(ip) {
                        ips.push(ip);
                    }
                }
            }
        }
    }

    // Parse /proc/net/if_inet6 for IPv6 addresses (Linux only)
    if let Ok(contents) = std::fs::read_to_string("/proc/net/if_inet6") {
        for line in contents.lines() {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 6 {
                let hex = parts[0];
                if hex.len() == 32 {
                    let mut bytes = [0u8; 16];
                    let mut valid = true;
                    for i in 0..16 {
                        match u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16) {
                            Ok(b) => bytes[i] = b,
                            Err(_) => {
                                valid = false;
                                break;
                            }
                        }
                    }
                    if valid {
                        let ip = IpAddr::V6(std::net::Ipv6Addr::from(bytes));
                        if !ip.is_loopback() && !ip.is_unspecified() {
                            // Skip link-local (fe80::)
                            if let IpAddr::V6(v6) = ip {
                                if (v6.segments()[0] & 0xffc0) == 0xfe80 {
                                    continue;
                                }
                            }
                            if seen.insert(ip) {
                                ips.push(ip);
                            }
                        }
                    }
                }
            }
        }
    }

    ips
}

// ─── Join Status ───────────────────────────────────────────────────────────

/// Phase of a join attempt.
#[derive(Debug, Clone)]
pub enum JoinPhase {
    Connecting { attempt: u32, max_attempts: u32 },
    Sending { attempt: u32, max_attempts: u32 },
    Sent,
    Failed { error: String },
}

/// Real-time status of a join attempt to a specific peer.
#[derive(Debug, Clone)]
pub struct JoinStatus {
    pub phase: JoinPhase,
    pub has_relay: bool,
    pub has_direct: bool,
    pub direct_addr_count: usize,
    pub updated_at: Instant,
}

// ─── Driver ─────────────────────────────────────────────────────────────────

/// iroh P2P network transport bridge.
///
/// Bridges the actorized distribution protocol (running on a swactor runtime)
/// to iroh's async QUIC transport. Runs on a caller-supplied Tokio [`Handle`]
/// in explicit call sites. [`Self::pump_inbound_to_actors`] and
/// [`Self::drain_outbox`] are pure-synchronous (they drain in-memory queues
/// filled by background accept/dial/reader/send tasks), so the node's async
/// driver loop can call them on a tokio worker. The constructor and the
/// synchronous [`Self::shutdown`] still `block_on` and must run on a non-async
/// thread; the async loop uses [`Self::close`] for teardown.
pub struct IrohDriver {
    /// This node's signing identity (reconstructed from the iroh endpoint
    /// secret). Signs `DirectoryEntry` claims for locally-spawned actors
    /// ([`Self::register_actor`]) and is the source of [`Self::node_id`].
    keypair: Keypair,
    endpoint: Endpoint,
    rt: Handle,
    connections: HashMap<NodeId, Connection>,
    peer_auth: Option<Arc<Mutex<PeerAllowList>>>,
    /// Collects connections from background join tasks.
    pending_joins: Arc<Mutex<Vec<JoinResult>>>,
    /// Peers with a background dial in flight. A cache-miss send checks this
    /// so it starts at most one dial per peer instead of blocking the SWIM
    /// pump on a synchronous 30s dial (fatal to failure detection: a probe to
    /// a dead peer would otherwise freeze the whole node for the dial budget).
    dialing: Arc<Mutex<HashSet<NodeId>>>,
    /// Connections accepted by the background accept loop (SWIM ALPN).
    accepted_conns: Arc<Mutex<Vec<(NodeId, Connection)>>>,
    /// Connections accepted on non-SWIM ALPNs (streams, etc.).
    other_accepted_conns: Arc<Mutex<Vec<(NodeId, Connection)>>>,
    /// Frames read by per-connection reader tasks, drained synchronously by
    /// `recv()` / `pump_inbound_to_actors()`. This decouples network reads from the
    /// state machine so `recv()`/`tick()` are pure-sync (no `block_on`) and can run
    /// inside the unified async driver loop on a tokio worker. Each entry is
    /// `(dest, type_tag, payload, from)` — `dest` is the destination actor address
    /// carried on the wire (`DIRECTORY.md` §5); the legacy `recv` path ignores it.
    incoming: Arc<Mutex<Vec<(ActorAddress, String, Vec<u8>, NodeId)>>>,
    /// Connections whose fire-and-forget send failed; evicted (and re-dialed)
    /// on the next `recv()`. Populated by the spawned send tasks.
    evict: Arc<Mutex<Vec<NodeId>>>,
    /// Relay URLs learned from join seeds, used for reconnection.
    peer_relay_urls: HashMap<NodeId, iroh::RelayUrl>,
    /// Real-time join status for each peer being joined.
    join_statuses: Arc<Mutex<HashMap<NodeId, JoinStatus>>>,
    /// Embedded relay server (if started).
    #[cfg(feature = "relay")]
    relay_server: Option<iroh_relay::server::Server>,
    /// URL of the embedded relay server (if started).
    relay_url: Option<String>,
    /// Actor-bridge wiring, installed via [`Self::enable_actor_bridge`]. When
    /// present, the driver decodes inbound frames into actor messages
    /// ([`Self::pump_inbound_to_actors`]) and writes actor-produced outbound
    /// frames drained from the shared outbox ([`Self::drain_outbox`]). Installed
    /// in production; `None` only in harnesses that route frames by hand.
    actor_bridge: Option<ActorBridge>,
    /// A tokio runtime owned by this driver, present only when the legacy
    /// [`Self::new`] convenience was called outside any tokio context and
    /// silently created one. Explicit construction via [`Self::with_handle`]
    /// always leaves this `None`.
    /// Declared last so it is dropped after the endpoint/connections on teardown.
    _owned_rt: Option<tokio::runtime::Runtime>,
}

/// State the driver needs to shuttle frames between iroh and the swactor runtime
/// once the protocol runs as actors (see [`IrohDriver::enable_actor_bridge`]).
struct ActorBridge {
    /// The swactor runtime, for `deliver_raw` of decoded inbound + `SendFailed`.
    rt: Arc<Runtime>,
    /// Actor codec: wire `type_tag` → the actor `Incoming` variant and back.
    codec: Arc<CodecRegistry>,
    /// `type_tag` → the local actor mailbox that owns it (ingress routing table).
    routes: HashMap<String, ActorAddress>,
    /// The SwimActor's address, for delivering `SendFailed{to}` on a write failure.
    swim_addr: ActorAddress,
    /// This node's own peer-mailbox address (`peer_addr(self)`). A frame whose wire
    /// `dest` equals this was gossip addressed to the node (route it by tag);
    /// anything else is a §5 application message addressed to a specific actor.
    self_peer_addr: ActorAddress,
    /// Per-peer relay URLs published by the `MetadataActor`, read on the dial path
    /// (the egress can't block to `ask` the actor).
    relay_mirror: RelayMirror,
    /// The directory's converged actor→host view, published by the `DirectoryActor`.
    /// Read for the `directory_route_count` snapshot field (observability only).
    route_view: RouteView,
}

impl IrohDriver {
    /// Legacy convenience constructor that adapts to the caller's tokio context.
    ///
    /// - **Inside a tokio runtime**: shares that ambient runtime.
    /// - **Outside any tokio runtime**: silently creates and owns a multi-threaded
    ///   runtime.
    ///
    /// Prefer [`Self::with_handle`] in new code so Tokio engine ownership is
    /// explicit. Either way the synchronous facade (`recv`/`tick`/`snapshot`/
    /// `shutdown`) must be driven from a non-async thread; methods that
    /// `block_on` will panic if called from inside the runtime they use.
    pub fn new(config: IrohDriverConfig) -> Result<Self, Box<dyn std::error::Error>> {
        match Handle::try_current() {
            Ok(handle) => Self::build(handle, None, config),
            Err(_) => {
                let owned = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .build()?;
                let handle = owned.handle().clone();
                Self::build(handle, Some(owned), config)
            }
        }
    }

    /// Create a new iroh driver bound to an explicit tokio runtime handle.
    ///
    /// The driver runs entirely on `rt` (a runtime it does **not** own — the
    /// caller keeps it alive): the background accept loop and dials are
    /// `rt.spawn`ed, and the synchronous facade (`recv`/`tick`/`snapshot`/
    /// `shutdown`) bridges to async via `rt.block_on`. **Those methods must
    /// therefore be driven from a non-async thread** (a dedicated pump thread),
    /// never from inside an async task running on `rt`, or `block_on` will panic.
    pub fn with_handle(
        rt: Handle,
        config: IrohDriverConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Self::build(rt, None, config)
    }

    /// Shared constructor body.
    ///
    /// `owned` carries a runtime the driver must keep alive (when [`Self::new`]
    /// created one because there was no ambient runtime), or `None` when running
    /// on a shared handle.
    ///
    /// If `embedded_relay_bind` is set (requires `relay` feature), an embedded
    /// relay server is started on `rt` before the endpoint is created. On success
    /// the endpoint uses the embedded relay; on failure it falls back to
    /// `config.relay_mode`.
    fn build(
        rt: Handle,
        owned: Option<tokio::runtime::Runtime>,
        config: IrohDriverConfig,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // Try to start embedded relay if configured
        #[cfg(feature = "relay")]
        let (relay_server, relay_url, effective_relay_mode) = match config.embedded_relay_bind {
            Some(bind_addr) => {
                match rt.block_on(start_embedded_relay(bind_addr, config.relay_public_ip)) {
                    Ok((server, url)) => {
                        let url_str = url.to_string();
                        (Some(server), Some(url_str), RelayMode::Custom(url.into()))
                    }
                    Err(_) => (None, None, config.relay_mode),
                }
            }
            None => (None, None, config.relay_mode),
        };
        #[cfg(not(feature = "relay"))]
        let (relay_url, effective_relay_mode) = (None::<String>, config.relay_mode);

        // A custom relay is operator-controlled (typically `iroh-driver-relay`
        // on a VPS, serving QUIC Address Discovery with a self-signed cert).
        // We trust its cert below so QAD's TLS handshake succeeds — without
        // that, address discovery fails and every connection stays
        // `conn_type=Relay`, which defeats hole-punching and makes a NAT'd peer
        // (e.g. a locally-run orchestrator) reachable only over the relay.
        let custom_relay = matches!(effective_relay_mode, RelayMode::Custom(_));

        let endpoint = rt.block_on(async {
            let mut alpns = vec![ALPN.to_vec()];
            alpns.extend(config.additional_alpns.iter().cloned());
            let mut builder = Endpoint::builder(iroh::endpoint::presets::Minimal)
                .relay_mode(effective_relay_mode)
                .alpns(alpns);

            // Only relax relay-cert verification for a custom relay; Default /
            // Staging relays keep full WebPKI verification.
            if custom_relay {
                builder = builder.ca_roots_config(iroh::tls::CaRootsConfig::insecure_skip_verify());
            }

            if let Some(key) = config.secret_key {
                builder = builder.secret_key(key);
            }

            builder.bind().await
        })?;

        // The driver's signing identity matches the iroh endpoint: both use
        // ed25519-dalek, so we reconstruct our Keypair from iroh's secret key.
        // (`config.node` carries the per-protocol configs the node binary already
        // unpacked to build the actors; the transport-only driver doesn't use it.)
        let iroh_secret = endpoint.secret_key().to_bytes();
        let keypair = Keypair::from_bytes(&iroh_secret);

        // Spawn background accept loop so incoming connections are never missed
        let accepted_conns: Arc<Mutex<Vec<(NodeId, Connection)>>> =
            Arc::new(Mutex::new(Vec::new()));
        let other_accepted_conns: Arc<Mutex<Vec<(NodeId, Connection)>>> =
            Arc::new(Mutex::new(Vec::new()));
        {
            let ep = endpoint.clone();
            let peer_auth = config.peer_auth.clone();
            let swim_buf = Arc::clone(&accepted_conns);
            let other_buf = Arc::clone(&other_accepted_conns);
            rt.spawn(async move {
                loop {
                    match ep.accept().await {
                        Some(incoming) => match incoming.await {
                            Ok(conn) => {
                                let remote_id = conn.remote_id();
                                let node_id = NodeId(*remote_id.as_bytes());
                                // Peer auth check
                                let allowed = match &peer_auth {
                                    None => true,
                                    Some(auth) => auth.lock().unwrap().is_allowed(&node_id),
                                };
                                if !allowed {
                                    conn.close(0u32.into(), b"unauthorized");
                                    continue;
                                }
                                // Route by negotiated ALPN
                                let negotiated_alpn = conn.alpn();
                                if negotiated_alpn == ALPN {
                                    swim_buf.lock().unwrap().push((node_id, conn));
                                } else {
                                    other_buf.lock().unwrap().push((node_id, conn));
                                }
                            }
                            Err(_) => {}
                        },
                        None => break, // endpoint closed
                    }
                }
            });
        }

        Ok(Self {
            keypair,
            endpoint,
            rt,
            connections: HashMap::new(),
            peer_auth: config.peer_auth,
            pending_joins: Arc::new(Mutex::new(Vec::new())),
            dialing: Arc::new(Mutex::new(HashSet::new())),
            accepted_conns,
            other_accepted_conns,
            incoming: Arc::new(Mutex::new(Vec::new())),
            evict: Arc::new(Mutex::new(Vec::new())),
            peer_relay_urls: HashMap::new(),
            join_statuses: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "relay")]
            relay_server,
            relay_url,
            actor_bridge: None,
            _owned_rt: owned,
        })
    }

    /// Get a handle to the tokio runtime this driver runs on (the outer,
    /// ambient runtime — the driver does not own it).
    pub fn tokio_handle(&self) -> tokio::runtime::Handle {
        self.rt.clone()
    }

    /// Get a reference to the iroh endpoint (for creating outbound connections).
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Drain connections accepted on non-SWIM ALPNs.
    pub fn drain_other_connections(&self) -> Vec<(NodeId, Connection)> {
        self.other_accepted_conns
            .lock()
            .unwrap()
            .drain(..)
            .collect()
    }

    /// The node's identity.
    pub fn node_id(&self) -> NodeId {
        self.keypair.node_id()
    }

    /// The endpoint's full address (public key + direct socket addresses).
    ///
    /// Constructs the address from the endpoint's public key and bound
    /// sockets. For sockets bound to `0.0.0.0`, emits one address per
    /// discovered LAN IP so that peers on the same network can connect
    /// directly. IPv6 unspecified is mapped to localhost.
    pub fn endpoint_addr(&self) -> EndpointAddr {
        let key = PublicKey::from_bytes(&self.keypair.node_id().0)
            .expect("node_id is a valid public key");
        let mut addr = EndpointAddr::new(key);
        for sa in self.direct_addresses() {
            addr = addr.with_ip_addr(sa);
        }
        addr
    }

    /// Compute direct socket addresses from bound sockets + LAN discovery.
    ///
    /// For sockets bound to `0.0.0.0`, emits one `SocketAddr` per discovered
    /// LAN IP using the bound port. Specific-IP binds are kept as-is.
    pub fn direct_addresses(&self) -> Vec<SocketAddr> {
        let lan_ips = discover_lan_ips();
        let mut addrs = Vec::new();
        for sock in self.endpoint.bound_sockets() {
            match sock.ip() {
                IpAddr::V4(ip) if ip.is_unspecified() => {
                    // Emit one address per discovered LAN IP
                    for lip in &lan_ips {
                        if lip.is_ipv4() {
                            addrs.push(SocketAddr::new(*lip, sock.port()));
                        }
                    }
                    // Also include localhost for same-host connectivity
                    addrs.push(SocketAddr::new(
                        IpAddr::V4(std::net::Ipv4Addr::LOCALHOST),
                        sock.port(),
                    ));
                }
                IpAddr::V6(ip) if ip.is_unspecified() => {
                    addrs.push(SocketAddr::new(
                        IpAddr::V6(std::net::Ipv6Addr::LOCALHOST),
                        sock.port(),
                    ));
                }
                _ => {
                    addrs.push(sock);
                }
            }
        }
        addrs
    }

    /// Sign a host claim for a locally-spawned actor, to hand to the
    /// `DirectoryActor` (`DirectoryIn::Register`) so peers learn this node hosts
    /// it. The driver holds the same ed25519 identity as the iroh endpoint, so
    /// the signed [`DirectoryEntry`](distribution::types::DirectoryEntry) verifies
    /// against this node's `NodeId`.
    pub fn register_actor(
        &self,
        actor_addr: ActorAddress,
        generation: u64,
    ) -> distribution::types::DirectoryEntry {
        self.keypair.sign_directory_entry(actor_addr, generation)
    }

    /// Capture the driver-owned slice of the node's observable state: identity,
    /// listen address, and the directory route-view extent. The core node no
    /// longer polls this (its telemetry flows over the datastream), so this is
    /// kept as a convenience over [`listen_addr`](Self::listen_addr) and
    /// [`directory_route_count`](Self::directory_route_count).
    pub fn snapshot(&self) -> DistributionNodeSnapshot {
        let mut snap = DistributionNodeSnapshot::empty(self.node_id());
        snap.listen_addr = Some(self.listen_addr());
        snap.directory_route_count = self.directory_route_count();
        snap
    }

    /// This node's listen / endpoint address. `endpoint.id()` is synchronous, so
    /// no runtime bridge is needed.
    pub fn listen_addr(&self) -> String {
        format!("{}", self.endpoint.id())
    }

    /// The number of actor→host routes this node currently knows — the converged
    /// directory `RouteView` extent. The view lives in the `DirectoryActor`; the
    /// driver holds a read-mirror of it, so report the mirror's length.
    pub fn directory_route_count(&self) -> usize {
        self.actor_bridge
            .as_ref()
            .and_then(|b| b.route_view.read().ok().map(|v| v.len()))
            .unwrap_or(0)
    }

    /// This node's location cache: every directory route whose host is a *remote*
    /// peer — the `(actor, host)` locations the node has learned in order to route
    /// across the network. Read off the same directory `RouteView` mirror as
    /// [`directory_route_count`](Self::directory_route_count), but filtered to
    /// peer-hosted actors: a node never needs to "cache" the location of an actor
    /// it hosts itself, so self-hosted routes are excluded. This is the honest
    /// `dist.state.cache_*` source, distinct from the all-routes count above.
    pub fn location_cache_entries(&self) -> Vec<(ActorAddress, NodeId)> {
        let self_id = self.node_id();
        self.actor_bridge
            .as_ref()
            .and_then(|b| {
                b.route_view.read().ok().map(|view| {
                    let mut entries: Vec<(ActorAddress, NodeId)> = view
                        .iter()
                        .filter(|(_, host)| **host != self_id)
                        .map(|(actor, host)| (*actor, *host))
                        .collect();
                    // Stable order so observers don't reshuffle each tick.
                    entries.sort_by(|a, b| a.0.0.cmp(&b.0.0));
                    entries
                })
            })
            .unwrap_or_default()
    }

    /// Get a snapshot of all join statuses.
    pub fn join_statuses(&self) -> HashMap<NodeId, JoinStatus> {
        self.join_statuses.lock().unwrap().clone()
    }

    /// Clear join statuses for the given node IDs (e.g. peers that are now alive).
    pub fn clear_join_statuses(&self, node_ids: &[NodeId]) {
        let mut map = self.join_statuses.lock().unwrap();
        for id in node_ids {
            map.remove(id);
        }
    }

    /// Clear a single join status entry.
    pub fn clear_join_status(&self, node_id: &NodeId) {
        self.join_statuses.lock().unwrap().remove(node_id);
    }

    /// Join a cluster by connecting to seed nodes via iroh.
    ///
    /// Each seed is identified by its `EndpointAddr` (public key + optional
    /// direct addresses). Connect+send is spawned as a background task so
    /// that the peer can accept the connection during its `recv()` cycle.
    /// Results are collected in the next `recv()` call.
    pub fn join(&mut self, seeds: &[EndpointAddr]) {
        for seed_addr in seeds {
            // Store relay URL for future reconnection
            let seed_node_id = NodeId(*seed_addr.id.as_bytes());
            if let Some(relay) = seed_addr.relay_urls().next() {
                self.peer_relay_urls.insert(seed_node_id, relay.clone());
            }
            // Drop stale cached connection so iroh establishes a fresh one.
            // (Clearing a Dead SWIM entry to accept the re-join is the SwimActor's
            // job now; the seed re-establishes via the JoinResponse it returns.)
            self.connections.remove(&seed_node_id);
            // Enrich the seed addr with a cached relay URL if it doesn't
            // have one. The re-peer flow sends only a bare public key
            // because metadata (including relay URL) is stripped when a
            // node is declared dead. Without a relay URL iroh cannot
            // reach the peer through NAT.
            let enriched = if seed_addr.relay_urls().next().is_none() {
                if let Some(relay) = self
                    .peer_relay_urls
                    .get(&seed_node_id)
                    .cloned()
                    .or_else(|| self.endpoint.addr().relay_urls().next().cloned())
                {
                    seed_addr.clone().with_relay_url(relay)
                } else {
                    seed_addr.clone()
                }
            } else {
                seed_addr.clone()
            };
            self.spawn_join_request(enriched);
        }
    }

    fn spawn_join_request(&self, seed_addr: EndpointAddr) {
        let msg = JoinRequest {
            from: self.node_id(),
        };
        let payload = serde_json::to_vec(&msg).expect("serialize JoinRequest");
        let tag = <JoinRequest as swactor_transport::NetworkMessage>::type_tag();
        let endpoint = self.endpoint.clone();
        let seed_node_id = NodeId(*seed_addr.id.as_bytes());
        let pending = Arc::clone(&self.pending_joins);
        let statuses = Arc::clone(&self.join_statuses);

        let has_relay = seed_addr.relay_urls().next().is_some();
        let direct_addr_count = seed_addr.ip_addrs().count();
        let has_direct = direct_addr_count > 0;

        self.rt.spawn(async move {
            let mut delay = Duration::from_secs(2);
            let max_delay = Duration::from_secs(30);
            let max_attempts: u32 = 5;
            let per_attempt_timeout = Duration::from_secs(10);

            for attempt in 1..=max_attempts {
                if attempt > 1 {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                }

                // Update status: Connecting
                {
                    let mut map = statuses.lock().unwrap();
                    map.insert(
                        seed_node_id,
                        JoinStatus {
                            phase: JoinPhase::Connecting {
                                attempt,
                                max_attempts,
                            },
                            has_relay,
                            has_direct,
                            direct_addr_count,
                            updated_at: Instant::now(),
                        },
                    );
                }

                let connect_result = tokio::time::timeout(
                    per_attempt_timeout,
                    endpoint.connect(seed_addr.clone(), ALPN),
                )
                .await;

                match connect_result {
                    Ok(Ok(conn)) => {
                        // Update status: Sending
                        {
                            let mut map = statuses.lock().unwrap();
                            map.insert(
                                seed_node_id,
                                JoinStatus {
                                    phase: JoinPhase::Sending {
                                        attempt,
                                        max_attempts,
                                    },
                                    has_relay,
                                    has_direct,
                                    direct_addr_count,
                                    updated_at: Instant::now(),
                                },
                            );
                        }

                        let send_result: Result<(), String> = async {
                            let mut send = conn.open_uni().await.map_err(|e| e.to_string())?;
                            // Frame format must match `read_message`: the 32-byte
                            // dest precedes the tag. A JoinRequest is gossip to the
                            // seed, so its dest is the seed's peer-mailbox (the
                            // receiver routes it by tag).
                            let dest = peer_addr(seed_node_id);
                            let tag_len = (tag.len() as u32).to_be_bytes();
                            send.write_all(&dest.0).await.map_err(|e| e.to_string())?;
                            send.write_all(&tag_len).await.map_err(|e| e.to_string())?;
                            send.write_all(tag.as_bytes())
                                .await
                                .map_err(|e| e.to_string())?;
                            send.write_all(&payload).await.map_err(|e| e.to_string())?;
                            send.finish().map_err(|e| e.to_string())?;
                            Ok(())
                        }
                        .await;

                        match send_result {
                            Ok(()) => {
                                // Update status: Sent
                                {
                                    let mut map = statuses.lock().unwrap();
                                    map.insert(
                                        seed_node_id,
                                        JoinStatus {
                                            phase: JoinPhase::Sent,
                                            has_relay,
                                            has_direct,
                                            direct_addr_count,
                                            updated_at: Instant::now(),
                                        },
                                    );
                                }
                                pending.lock().unwrap().push(JoinResult {
                                    node_id: seed_node_id,
                                    conn,
                                });
                                return;
                            }
                            Err(_) => {
                                continue;
                            }
                        }
                    }
                    Ok(Err(_)) => {
                        continue;
                    }
                    Err(_) => {
                        continue;
                    }
                }
            }
            // Update status: Failed
            {
                let mut map = statuses.lock().unwrap();
                map.insert(
                    seed_node_id,
                    JoinStatus {
                        phase: JoinPhase::Failed {
                            error: "all attempts exhausted".into(),
                        },
                        has_relay,
                        has_direct,
                        direct_addr_count,
                        updated_at: Instant::now(),
                    },
                );
            }
        });
    }

    /// Spawn a persistent reader task for `conn` that pushes every framed SWIM
    /// message into the shared `incoming` queue until the connection closes.
    /// Reads run on the tokio pool; `recv()` only drains the queue — that is
    /// what keeps it `block_on`-free.
    fn spawn_reader(&self, node_id: NodeId, conn: Connection) {
        let incoming = Arc::clone(&self.incoming);
        self.rt.spawn(async move {
            loop {
                match conn.accept_uni().await {
                    Ok(mut recv) => match read_message(&mut recv).await {
                        Ok((dest, tag, payload)) => {
                            incoming.lock().unwrap().push((dest, tag, payload, node_id));
                        }
                        Err(_) => break,
                    },
                    Err(_) => break, // connection closed
                }
            }
        });
    }

    /// Cache a connection and start reading from it.
    fn cache_connection(&mut self, node_id: NodeId, conn: Connection) {
        self.spawn_reader(node_id, conn.clone());
        self.connections.insert(node_id, conn);
    }

    /// Fold completed background dials/joins and accepted connections into the
    /// connection cache (spawning readers), then evict + re-dial connections
    /// whose fire-and-forget send failed. Shared by the legacy `recv` and the
    /// actor ingress pump ([`Self::pump_inbound_to_actors`]).
    fn fold_connections(&mut self) {
        // Fold completed background join connections into the cache (+ read).
        let joins: Vec<JoinResult> = self.pending_joins.lock().unwrap().drain(..).collect();
        for result in joins {
            self.cache_connection(result.node_id, result.conn);
        }

        // Fold connections accepted from remote peers into the cache (+ read).
        let accepted: Vec<(NodeId, Connection)> =
            self.accepted_conns.lock().unwrap().drain(..).collect();
        for (node_id, conn) in accepted {
            self.cache_connection(node_id, conn);
        }

        // Evict connections whose fire-and-forget send failed; kick a fresh dial.
        let evicted: Vec<NodeId> = self.evict.lock().unwrap().drain(..).collect();
        for node_id in evicted {
            self.connections.remove(&node_id);
            if let Ok(key) = PublicKey::from_bytes(&node_id.0) {
                let _ = self.get_or_connect(node_id, key);
            }
        }
    }

    // ─── Actor bridge: iroh ⇄ swactor runtime ─────────────────────────

    /// Install the actor-bridge wiring so the driver shuttles frames between iroh
    /// and the swactor runtime ([`Self::pump_inbound_to_actors`] /
    /// [`Self::drain_outbox`]) — the seam by which the protocol actors send and
    /// receive over iroh.
    pub fn enable_actor_bridge(
        &mut self,
        rt: Arc<Runtime>,
        codec: Arc<CodecRegistry>,
        routes: HashMap<String, ActorAddress>,
        swim_addr: ActorAddress,
        relay_mirror: RelayMirror,
        route_view: RouteView,
    ) {
        let self_peer_addr = peer_addr(self.node_id());
        self.actor_bridge = Some(ActorBridge {
            rt,
            codec,
            routes,
            swim_addr,
            self_peer_addr,
            relay_mirror,
            route_view,
        });
    }

    /// Drain the actors' outbound queue and write each frame to iroh. Runs on the
    /// driver-loop thread (it owns the connection cache); the actual stream write
    /// is fire-and-forget on the tokio pool, so this never blocks.
    pub fn drain_outbox(&mut self, outbox: &Outbox) {
        let frames: Vec<distribution::transport_bridge::OutFrame> =
            outbox.lock().unwrap().drain(..).collect();
        for f in frames {
            self.send_wire(f.to, f.dest, &f.type_tag, f.payload);
        }
    }

    /// Ingress: fold new connections, then decode each received frame and
    /// `deliver_raw` it to the right actor. Pure-sync.
    ///
    /// Two kinds of frame arrive on one stream, told apart by the wire `dest`
    /// (`DIRECTORY.md` §5):
    ///  - **Gossip to a well-known protocol actor** was addressed to this node's
    ///    peer-mailbox (`dest == peer_addr(self)`); it is routed by `type_tag` to
    ///    the local actor that owns it (SWIM / registry / metadata / directory).
    ///  - **An application message** routed by the directory carries the target
    ///    actor's own address as `dest`; it is delivered straight into that actor's
    ///    mailbox. A `dest` for an actor that isn't local here drops best-effort.
    pub fn pump_inbound_to_actors(&mut self) {
        self.fold_connections();
        let messages: Vec<(ActorAddress, String, Vec<u8>, NodeId)> =
            self.incoming.lock().unwrap().drain(..).collect();
        let Some(bridge) = self.actor_bridge.as_ref() else {
            return;
        };
        for (dest, tag, payload, _from) in messages {
            let Ok(boxed) = bridge.codec.decode(&tag, &payload) else {
                continue;
            };
            if dest == bridge.self_peer_addr {
                // Gossip: route by tag to the protocol actor that owns it.
                if let Some(&addr) = bridge.routes.get(&tag) {
                    let _ = bridge.rt.deliver_raw(addr, boxed);
                }
            } else {
                // Application message: deliver straight to the addressed actor.
                let _ = bridge.rt.deliver_raw(dest, boxed);
            }
        }
    }

    /// Write an already-encoded frame (`type_tag` + `payload`) to `to` over iroh,
    /// fire-and-forget. On a stream-write failure the connection is evicted and,
    /// for SWIM frames, `SendFailed{to}` is delivered to the SwimActor (§4.3).
    fn send_wire(&mut self, to: NodeId, dest: ActorAddress, type_tag: &str, payload: Vec<u8>) {
        let target_key = match PublicKey::from_bytes(&to.0) {
            Ok(k) => k,
            Err(_) => return,
        };
        let conn = match self.get_or_connect(to, target_key) {
            Ok(c) => c,
            // No ready connection: a background dial was kicked. Drop best-effort
            // — the actor re-sends on a later tick (a dead peer is still detected
            // via probe/Ack timeout, not masked by a blocking dial).
            Err(_) => return,
        };
        let evict = Arc::clone(&self.evict);
        let tag = type_tag.to_string();
        // Only SWIM frames feed failure detection via SendFailed; gossip is
        // best-effort and just drops.
        let send_failed = if is_swim_tag(type_tag) {
            self.actor_bridge
                .as_ref()
                .map(|b| (b.rt.clone(), b.swim_addr))
        } else {
            None
        };
        self.rt.spawn(async move {
            let result: Result<(), Box<dyn std::error::Error>> = async {
                let mut send = conn.open_uni().await?;
                write_message(&mut send, dest, tag.as_bytes(), &payload).await?;
                send.finish()?;
                Ok(())
            }
            .await;
            if result.is_err() {
                evict.lock().unwrap().push(to);
                if let Some((rt, swim_addr)) = send_failed {
                    let _ = rt.deliver_raw(swim_addr, Box::new(SwimIn::SendFailed { to }));
                }
            }
        });
    }

    fn get_or_connect(
        &mut self,
        node_id: NodeId,
        key: PublicKey,
    ) -> Result<Connection, Box<dyn std::error::Error>> {
        // Defense in depth: check peer auth before connecting
        if !self.is_peer_allowed(&node_id) {
            return Err(format!(
                "peer {} not in allow-list",
                swactor_transport::hex_encode(&node_id.0[..4])
            )
            .into());
        }

        // Check for cached connection that's still open
        if let Some(conn) = self.connections.get(&node_id) {
            if conn.close_reason().is_none() {
                return Ok(conn.clone());
            }
            // Connection closed, remove it
            self.connections.remove(&node_id);
        }

        // Resolve relay URL: explicit cache → MetadataActor mirror → legacy SWIM
        // metadata (retained node) → own home relay.
        let relay = if let Some(r) = self.peer_relay_urls.get(&node_id).cloned() {
            Some(r)
        } else if let Some(r) = self
            .actor_bridge
            .as_ref()
            .and_then(|b| b.relay_mirror.read().ok()?.get(&node_id).cloned())
            .and_then(|s| s.parse::<iroh::RelayUrl>().ok())
        {
            Some(r)
        } else if let Some(r) = self.endpoint.addr().relay_urls().next().cloned() {
            Some(r)
        } else {
            None
        };

        // Hand the dial to a background task instead of blocking the SWIM
        // pump. A synchronous dial of up to ATTEMPTS × per-attempt-timeout
        // (tens of seconds) to an unreachable peer would freeze the whole node
        // — catastrophic for failure detection, which is exactly when peers go
        // unreachable. The connection lands in `pending_joins` and is folded
        // into the cache by the next `recv()`; this send is dropped
        // best-effort and SWIM re-sends over the cached connection on a later
        // tick (a genuinely dead peer is still detected via its probe/Ack
        // timeout, no longer masked by a 30s blocking dial).
        let dial_addr = match &relay {
            Some(r) => EndpointAddr::new(key).with_relay_url(r.clone()),
            None => EndpointAddr::new(key),
        };
        self.spawn_connect(node_id, dial_addr);
        Err("connection not ready; background dial started".into())
    }

    /// Dial `node_id` in the background (never blocks the SWIM pump),
    /// mirroring `spawn_join_request`'s retry/backoff but
    /// without sending a join payload. At most one dial runs per peer at a
    /// time (`dialing` guards re-entry); on success the connection is queued in
    /// `pending_joins` for `recv()` to cache, and the in-flight flag is always
    /// cleared when the task ends. The WAN-tuned 3 × 10s budget is preserved —
    /// it just no longer stalls the caller.
    fn spawn_connect(&self, node_id: NodeId, dial_addr: EndpointAddr) {
        if !self.dialing.lock().unwrap().insert(node_id) {
            return; // a dial is already in flight for this peer
        }
        let endpoint = self.endpoint.clone();
        let pending = Arc::clone(&self.pending_joins);
        let dialing = Arc::clone(&self.dialing);
        self.rt.spawn(async move {
            const ATTEMPTS: u32 = 3;
            let per_attempt_timeout = Duration::from_secs(10);
            for attempt in 1..=ATTEMPTS {
                let result = tokio::time::timeout(
                    per_attempt_timeout,
                    endpoint.connect(dial_addr.clone(), ALPN),
                )
                .await;
                if let Ok(Ok(conn)) = result {
                    pending.lock().unwrap().push(JoinResult { node_id, conn });
                    break;
                }
                if attempt < ATTEMPTS {
                    let backoff = if attempt == 1 { 200 } else { 600 };
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                }
            }
            dialing.lock().unwrap().remove(&node_id);
        });
    }

    // ─── Incoming: iroh → handler ────────────────────────────────────

    fn is_peer_allowed(&self, node_id: &NodeId) -> bool {
        match &self.peer_auth {
            None => true,
            Some(auth) => auth.lock().unwrap().is_allowed(node_id),
        }
    }

    /// URL of the embedded relay server, if one was started.
    pub fn relay_url(&self) -> Option<&str> {
        self.relay_url.as_deref()
    }

    /// The endpoint's home relay URL (from RelayMode::Custom), if connected.
    pub fn home_relay_url(&self) -> Option<iroh::RelayUrl> {
        self.endpoint.addr().relay_urls().next().cloned()
    }

    /// Async teardown for the unified driver loop, which runs on a tokio worker
    /// where `block_on` would panic. Mirrors [`Self::shutdown`] without blocking.
    pub async fn close(&mut self) {
        #[cfg(feature = "relay")]
        if let Some(server) = self.relay_server.take() {
            let _ = server.shutdown().await;
        }
        self.endpoint.close().await;
    }

    /// Shut down the driver: stop the embedded relay (if any), then close the
    /// iroh endpoint. Synchronous (`block_on`); call from a non-async thread
    /// (the test harness / standalone binaries). The node's async driver loop
    /// uses [`Self::close`] instead.
    pub fn shutdown(&mut self) {
        // Shut down embedded relay first (must stop before endpoint closes)
        #[cfg(feature = "relay")]
        if let Some(server) = self.relay_server.take() {
            self.rt.block_on(async {
                let _ = server.shutdown().await;
            });
        }
        self.rt.block_on(async {
            self.endpoint.close().await;
        });
    }
}

// ─── Embedded Relay ─────────────────────────────────────────────────────────

#[cfg(feature = "relay")]
async fn start_embedded_relay(
    bind_addr: std::net::SocketAddr,
    public_ip: Option<std::net::IpAddr>,
) -> Result<(iroh_relay::server::Server, iroh::RelayUrl), Box<dyn std::error::Error>> {
    let server = iroh_relay::server::Server::spawn(iroh_relay::server::ServerConfig::<(), ()> {
        relay: Some(iroh_relay::server::RelayConfig {
            http_bind_addr: bind_addr,
            tls: None,
            limits: Default::default(),
            key_cache_capacity: Some(256),
            access: iroh_relay::server::AccessConfig::Everyone,
        }),
        quic: None,
        metrics_addr: None,
    })
    .await?;

    let url: iroh::RelayUrl = match server.http_addr() {
        Some(addr) => {
            let host = public_ip.unwrap_or_else(|| addr.ip());
            format!("http://{}:{}/", host, addr.port()).parse()?
        }
        None => return Err("relay server has no HTTP address".into()),
    };

    Ok((server, url))
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

/// Whether a wire `type_tag` is a SWIM message — the frames whose write failure
/// must feed failure detection (`SendFailed`). Gossip frames are best-effort.
fn is_swim_tag(tag: &str) -> bool {
    matches!(
        tag,
        "swactor_dist::Ping"
            | "swactor_dist::Ack"
            | "swactor_dist::PingReq"
            | "swactor_dist::IndirectAck"
            | "swactor_dist::JoinRequest"
            | "swactor_dist::JoinResponse"
    )
}

// ─── Wire Framing Over QUIC Streams ─────────────────────────────────────────

/// Write a tagged message to a QUIC send stream.
///
/// Frame format: `[32B dest][4B tag_len][tag_bytes][payload_bytes]`. `dest` is the
/// destination actor address (`DIRECTORY.md` §5): the peer's mailbox for gossip,
/// or a specific actor for a directory-routed application message.
async fn write_message(
    send: &mut iroh::endpoint::SendStream,
    dest: ActorAddress,
    tag: &[u8],
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let tag_len = (tag.len() as u32).to_be_bytes();
    send.write_all(&dest.0).await?;
    send.write_all(&tag_len).await?;
    send.write_all(tag).await?;
    send.write_all(payload).await?;
    Ok(())
}

/// Read a tagged message from a QUIC recv stream.
///
/// Returns `(dest, type_tag, payload)` (see [`write_message`] for the frame format).
async fn read_message(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<(ActorAddress, String, Vec<u8>), Box<dyn std::error::Error>> {
    let mut dest_buf = [0u8; 32];
    recv.read_exact(&mut dest_buf).await?;
    let dest = ActorAddress(dest_buf);

    let mut tag_len_buf = [0u8; 4];
    recv.read_exact(&mut tag_len_buf).await?;
    let tag_len = u32::from_be_bytes(tag_len_buf) as usize;

    if tag_len > 1024 {
        return Err("tag too large".into());
    }

    let mut tag_buf = vec![0u8; tag_len];
    recv.read_exact(&mut tag_buf).await?;
    let tag = String::from_utf8(tag_buf)?;

    let payload = recv.read_to_end(64 * 1024).await?;

    Ok((dest, tag, payload))
}
