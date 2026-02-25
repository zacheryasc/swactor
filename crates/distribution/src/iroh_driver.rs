//! iroh-based P2P network driver for `DistributedNode`.
//!
//! Provides the same driver pattern as `NodeDriver` (TCP), but uses iroh's
//! QUIC-based peer-to-peer transport with built-in TLS, NAT hole-punching,
//! and relay server fallback.
//!
//! The driver owns a tokio runtime internally, exposing a synchronous API
//! (`tick()`, `recv()`, `join()`) to match the existing main loop pattern.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use iroh::endpoint::Connection;
use iroh::{Endpoint, EndpointAddr, PublicKey, RelayMode, SecretKey};
use tokio::runtime::Runtime as TokioRuntime;

use crate::crypto::Keypair;
use crate::messages::*;
use crate::node::{DistributedNode, DistributedNodeConfig};
use crate::peer_auth::PeerAllowList;
use crate::snapshot::DistributionNodeSnapshot;
use crate::swim::node::NodeAction;
use crate::types::NodeId;

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

/// Discover all non-loopback LAN IP addresses on this host.
///
/// Uses UDP socket tricks to multiple broadcast destinations to find
/// addresses across different subnets. Also parses `/proc/net/if_inet6`
/// for IPv6 addresses on Linux.
pub fn discover_lan_ips() -> Vec<IpAddr> {
    let mut ips = Vec::new();
    let mut seen = std::collections::HashSet::new();

    // UDP socket trick: connect to a broadcast-ish address, read local_addr
    let targets: &[&str] = &[
        "10.255.255.255:1",
        "192.168.255.255:1",
        "172.31.255.255:1",
    ];
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
                            Err(_) => { valid = false; break; }
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

/// iroh P2P network driver.
///
/// Bridges the synchronous `DistributedNode` state machine with iroh's
/// async QUIC transport. Owns a tokio runtime internally.
pub struct IrohDriver {
    node: DistributedNode,
    endpoint: Endpoint,
    rt: TokioRuntime,
    connections: HashMap<NodeId, Connection>,
    peer_auth: Option<Arc<Mutex<PeerAllowList>>>,
    /// Collects connections from background join tasks.
    pending_joins: Arc<Mutex<Vec<JoinResult>>>,
    /// Connections accepted by the background accept loop (SWIM ALPN).
    accepted_conns: Arc<Mutex<Vec<(NodeId, Connection)>>>,
    /// Connections accepted on non-SWIM ALPNs (streams, etc.).
    other_accepted_conns: Arc<Mutex<Vec<(NodeId, Connection)>>>,
    /// Relay URLs learned from join seeds, used for reconnection.
    peer_relay_urls: HashMap<NodeId, iroh::RelayUrl>,
    /// Real-time join status for each peer being joined.
    join_statuses: Arc<Mutex<HashMap<NodeId, JoinStatus>>>,
    /// Embedded relay server (if started).
    #[cfg(feature = "relay")]
    relay_server: Option<iroh_relay::server::Server>,
    /// URL of the embedded relay server (if started).
    relay_url: Option<String>,
}

impl IrohDriver {
    /// Create a new iroh driver.
    ///
    /// Builds a tokio runtime, creates an iroh `Endpoint`, and initializes
    /// the protocol-layer `DistributedNode`.
    ///
    /// If `embedded_relay_bind` is set (requires `relay` feature), the driver
    /// starts an embedded relay server on the tokio runtime before creating
    /// the endpoint. On success the endpoint uses the embedded relay; on
    /// failure it falls back to `config.relay_mode`.
    pub fn new(config: IrohDriverConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;

        // Try to start embedded relay if configured
        #[cfg(feature = "relay")]
        let (relay_server, relay_url, effective_relay_mode) = match config.embedded_relay_bind {
            Some(bind_addr) => {
                match rt.block_on(start_embedded_relay(bind_addr, config.relay_public_ip)) {
                    Ok((server, url)) => {
                        let url_str = url.to_string();
                        eprintln!("Relay: embedded relay started at {url}");
                        (Some(server), Some(url_str), RelayMode::Custom(url.into()))
                    }
                    Err(e) => {
                        eprintln!("Relay: failed to start embedded relay: {e}, falling back");
                        (None, None, config.relay_mode)
                    }
                }
            }
            None => (None, None, config.relay_mode),
        };
        #[cfg(not(feature = "relay"))]
        let (relay_url, effective_relay_mode) = (None::<String>, config.relay_mode);

        let endpoint = rt.block_on(async {
            let mut alpns = vec![ALPN.to_vec()];
            alpns.extend(config.additional_alpns.iter().cloned());
            let mut builder = Endpoint::empty_builder(effective_relay_mode)
                .alpns(alpns);

            if let Some(key) = config.secret_key {
                builder = builder.secret_key(key);
            }

            builder.bind().await
        })?;

        // Create a DistributedNode whose identity matches the iroh endpoint.
        // Both use ed25519-dalek, so we can reconstruct our Keypair from iroh's secret key.
        let iroh_secret = endpoint.secret_key().to_bytes();
        let keypair = Keypair::from_bytes(&iroh_secret);
        let node = DistributedNode::with_keypair(keypair, config.node);

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
                                    eprintln!(
                                        "iroh driver: rejected connection from unauthorized peer {}",
                                        swactor::transport::hex_encode(&node_id.0[..4])
                                    );
                                    conn.close(0u32.into(), b"unauthorized");
                                    continue;
                                }
                                // Route by negotiated ALPN
                                let negotiated_alpn = conn.alpn();
                                if negotiated_alpn == ALPN {
                                    eprintln!(
                                        "iroh driver: accepted SWIM connection from {}",
                                        swactor::transport::hex_encode(&node_id.0[..4])
                                    );
                                    swim_buf.lock().unwrap().push((node_id, conn));
                                } else {
                                    eprintln!(
                                        "iroh driver: accepted non-SWIM connection from {} (ALPN: {})",
                                        swactor::transport::hex_encode(&node_id.0[..4]),
                                        String::from_utf8_lossy(negotiated_alpn),
                                    );
                                    other_buf.lock().unwrap().push((node_id, conn));
                                }
                            }
                            Err(e) => {
                                eprintln!("iroh driver: incoming connection error: {e}");
                            }
                        },
                        None => break, // endpoint closed
                    }
                }
            });
        }

        Ok(Self {
            node,
            endpoint,
            rt,
            connections: HashMap::new(),
            peer_auth: config.peer_auth,
            pending_joins: Arc::new(Mutex::new(Vec::new())),
            accepted_conns,
            other_accepted_conns,
            peer_relay_urls: HashMap::new(),
            join_statuses: Arc::new(Mutex::new(HashMap::new())),
            #[cfg(feature = "relay")]
            relay_server,
            relay_url,
        })
    }

    /// Get a handle to the tokio runtime owned by this driver.
    pub fn tokio_handle(&self) -> tokio::runtime::Handle {
        self.rt.handle().clone()
    }

    /// Get a reference to the iroh endpoint (for creating outbound connections).
    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// Drain connections accepted on non-SWIM ALPNs.
    pub fn drain_other_connections(&self) -> Vec<(NodeId, Connection)> {
        self.other_accepted_conns.lock().unwrap().drain(..).collect()
    }

    /// The node's identity.
    pub fn node_id(&self) -> NodeId {
        self.node.node_id()
    }

    /// The endpoint's full address (public key + direct socket addresses).
    ///
    /// Constructs the address from the endpoint's public key and bound
    /// sockets. For sockets bound to `0.0.0.0`, emits one address per
    /// discovered LAN IP so that peers on the same network can connect
    /// directly. IPv6 unspecified is mapped to localhost.
    pub fn endpoint_addr(&self) -> EndpointAddr {
        let key = PublicKey::from_bytes(&self.node.node_id().0)
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

    /// Access the underlying node (read-only).
    pub fn node(&self) -> &DistributedNode {
        &self.node
    }

    /// Access the underlying node (mutable).
    pub fn node_mut(&mut self) -> &mut DistributedNode {
        &mut self.node
    }

    /// Capture a snapshot enriched with iroh endpoint info.
    pub fn snapshot(&self) -> DistributionNodeSnapshot {
        let mut snap = self.node.snapshot();
        // Use iroh endpoint address as the "listen address"
        let addr_info = self.rt.block_on(async {
            format!("{}", self.endpoint.id())
        });
        snap.listen_addr = Some(addr_info);
        snap
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
            // Clear any Dead entry so the JoinResponse can re-establish it.
            // Without this, SWIM merge semantics reject Alive at the same
            // incarnation when the local entry is Dead (Dead > Alive).
            self.node.clear_dead_member(seed_node_id);
            // Drop stale cached connection so iroh establishes a fresh one
            self.connections.remove(&seed_node_id);
            // Enrich the seed addr with a cached relay URL if it doesn't
            // have one. The re-peer flow sends only a bare public key
            // because metadata (including relay URL) is stripped when a
            // node is declared dead. Without a relay URL iroh cannot
            // reach the peer through NAT.
            let enriched = if seed_addr.relay_urls().next().is_none() {
                if let Some(relay) = self.peer_relay_urls.get(&seed_node_id).cloned()
                    .or_else(|| self.node.relay_url(&seed_node_id)
                        .and_then(|s| s.parse::<iroh::RelayUrl>().ok()))
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
            from: self.node.node_id(),
        };
        let payload = serde_json::to_vec(&msg).expect("serialize JoinRequest");
        let tag = <JoinRequest as swactor::transport::NetworkMessage>::type_tag();
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

            for attempt in 1..=max_attempts {
                if attempt > 1 {
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(max_delay);
                }

                // Update status: Connecting
                {
                    let mut map = statuses.lock().unwrap();
                    map.insert(seed_node_id, JoinStatus {
                        phase: JoinPhase::Connecting { attempt, max_attempts },
                        has_relay,
                        has_direct,
                        direct_addr_count,
                        updated_at: Instant::now(),
                    });
                }

                eprintln!("iroh driver: join attempt {attempt}/{max_attempts} connecting to {}...", seed_addr.id);
                let connect_result = tokio::time::timeout(
                    Duration::from_secs(10),
                    endpoint.connect(seed_addr.clone(), ALPN),
                ).await;

                match connect_result {
                    Ok(Ok(conn)) => {
                        // Update status: Sending
                        {
                            let mut map = statuses.lock().unwrap();
                            map.insert(seed_node_id, JoinStatus {
                                phase: JoinPhase::Sending { attempt, max_attempts },
                                has_relay,
                                has_direct,
                                direct_addr_count,
                                updated_at: Instant::now(),
                            });
                        }

                        eprintln!("iroh driver: join attempt {attempt}/{max_attempts} connected to {}, sending...", seed_addr.id);
                        let send_result: Result<(), String> = async {
                            let mut send = conn.open_uni().await.map_err(|e| e.to_string())?;
                            let tag_len = (tag.len() as u32).to_be_bytes();
                            send.write_all(&tag_len).await.map_err(|e| e.to_string())?;
                            send.write_all(tag.as_bytes()).await.map_err(|e| e.to_string())?;
                            send.write_all(&payload).await.map_err(|e| e.to_string())?;
                            send.finish().map_err(|e| e.to_string())?;
                            Ok(())
                        }
                        .await;

                        match send_result {
                            Ok(()) => {
                                eprintln!("iroh driver: join attempt {attempt}/{max_attempts} sent to {}", seed_addr.id);
                                // Update status: Sent
                                {
                                    let mut map = statuses.lock().unwrap();
                                    map.insert(seed_node_id, JoinStatus {
                                        phase: JoinPhase::Sent,
                                        has_relay,
                                        has_direct,
                                        direct_addr_count,
                                        updated_at: Instant::now(),
                                    });
                                }
                                pending.lock().unwrap().push(JoinResult {
                                    node_id: seed_node_id,
                                    conn,
                                });
                                return;
                            }
                            Err(e) => {
                                eprintln!(
                                    "iroh driver: join attempt {attempt}/{max_attempts} send error to {}: {e}",
                                    seed_addr.id
                                );
                                continue;
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        eprintln!(
                            "iroh driver: join attempt {attempt}/{max_attempts} connect error to {}: {e}",
                            seed_addr.id
                        );
                        continue;
                    }
                    Err(_) => {
                        eprintln!(
                            "iroh driver: join attempt {attempt}/{max_attempts} connect timeout to {}",
                            seed_addr.id
                        );
                        continue;
                    }
                }
            }
            // Update status: Failed
            {
                let mut map = statuses.lock().unwrap();
                map.insert(seed_node_id, JoinStatus {
                    phase: JoinPhase::Failed { error: "all attempts exhausted".into() },
                    has_relay,
                    has_direct,
                    direct_addr_count,
                    updated_at: Instant::now(),
                });
            }
            eprintln!("iroh driver: join failed after {max_attempts} attempts to {}", seed_addr.id);
        });
    }

    /// Advance the node by one tick.
    pub fn tick(&mut self) {
        let actions = self.node.tick();
        self.send_actions(&actions);
    }

    /// Process incoming iroh connections and messages (non-blocking).
    pub fn recv(&mut self) {
        // Collect completed background join connections
        {
            let mut pending = self.pending_joins.lock().unwrap();
            if !pending.is_empty() {
                eprintln!("iroh driver: collecting {} pending join connection(s)", pending.len());
            }
            for result in pending.drain(..) {
                self.connections.insert(result.node_id, result.conn);
            }
        }

        let (incoming, new_conns) = self.rt.block_on(async {
            self.receive_pending().await
        });
        // Cache connections accepted from remote peers (replace stale ones)
        for (node_id, conn) in new_conns {
            self.connections.insert(node_id, conn);
        }
        for (tag, payload, from_key) in incoming {
            let from = NodeId(*from_key.as_bytes());
            let response_actions = self.dispatch_incoming(&tag, &payload, from);
            self.send_actions(&response_actions);
        }
    }

    // ─── Outgoing: NodeAction → iroh ─────────────────────────────────

    fn send_actions(&mut self, actions: &[NodeAction]) {
        let mut failure_targets: Vec<NodeId> = Vec::new();
        for action in actions {
            if let Err(e) = self.send_action(action) {
                eprintln!("iroh driver: send error: {e}");
                if let Some(target) = action_target(action) {
                    if !failure_targets.contains(&target) {
                        failure_targets.push(target);
                    }
                }
            }
        }
        for target in failure_targets {
            let probe_actions = self.node.report_send_failure(target);
            // Best-effort send of probe actions — no recursion on failure
            for action in &probe_actions {
                if let Err(e) = self.send_action(action) {
                    eprintln!("iroh driver: probe send error: {e}");
                }
            }
        }
    }

    fn send_action(&mut self, action: &NodeAction) -> Result<(), Box<dyn std::error::Error>> {
        match action {
            NodeAction::SendPing {
                to,
                sequence,
                piggyback,
            } => {
                let msg = Ping {
                    from: self.node.node_id(),
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_message(to, &msg)
            }

            NodeAction::SendAck {
                to,
                sequence,
                piggyback,
            } => {
                let msg = Ack {
                    from: self.node.node_id(),
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_message(to, &msg)
            }

            NodeAction::SendPingReq {
                relay,
                target,
                sequence,
                piggyback,
            } => {
                let msg = PingReq {
                    from: self.node.node_id(),
                    target: *target,
                    sequence: *sequence,
                    piggyback: piggyback.clone(),
                };
                self.send_message(relay, &msg)
            }

            NodeAction::SendJoinResponse { to, members } => {
                let msg = JoinResponse {
                    members: members.clone(),
                };
                self.send_message(to, &msg)
            }

            NodeAction::ForwardAck { to, target, sequence, piggyback } => {
                let msg = IndirectAck { target: *target, sequence: *sequence, piggyback: piggyback.clone() };
                self.send_message(to, &msg)
            }

            NodeAction::MembershipChanged { .. } => Ok(()),
        }
    }

    fn send_message<M: swactor::transport::NetworkMessage + serde::Serialize>(
        &mut self,
        to: &NodeId,
        msg: &M,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let tag = M::type_tag();
        let payload = serde_json::to_vec(msg)?;
        let target_key = PublicKey::from_bytes(&to.0)?;

        let conn = self.get_or_connect(*to, target_key)?;

        let result = self.rt.block_on(async {
            let mut send = conn.open_uni().await?;
            write_message(&mut send, tag.as_bytes(), &payload).await?;
            send.finish()?;
            Ok::<_, Box<dyn std::error::Error>>(())
        });

        if let Err(_) = result {
            // Connection may be stale, remove and retry once
            self.connections.remove(to);
            let conn = self.get_or_connect(*to, target_key)?;
            self.rt.block_on(async {
                let mut send = conn.open_uni().await?;
                write_message(&mut send, tag.as_bytes(), &payload).await?;
                send.finish()?;
                Ok::<_, Box<dyn std::error::Error>>(())
            })?;
        }

        Ok(())
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
                swactor::transport::hex_encode(&node_id.0[..4])
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

        // Resolve relay URL: explicit cache → SWIM metadata gossip → own home relay
        let relay = self.peer_relay_urls.get(&node_id).cloned()
            .or_else(|| {
                self.node
                    .relay_url(&node_id)
                    .and_then(|s| s.parse::<iroh::RelayUrl>().ok())
            })
            .or_else(|| self.endpoint.addr().relay_urls().next().cloned());

        let endpoint = self.endpoint.clone();
        let connect_timeout = Duration::from_secs(2);
        let conn = if let Some(relay) = relay {
            let addr = EndpointAddr::new(key).with_relay_url(relay);
            self.rt.block_on(async {
                match tokio::time::timeout(connect_timeout, endpoint.connect(addr, ALPN)).await {
                    Ok(result) => result.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) }),
                    Err(_) => Err("connect timeout".into()),
                }
            })?
        } else {
            self.rt.block_on(async {
                match tokio::time::timeout(connect_timeout, endpoint.connect(key, ALPN)).await {
                    Ok(result) => result.map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) }),
                    Err(_) => Err("connect timeout".into()),
                }
            })?
        };

        self.connections.insert(node_id, conn.clone());
        Ok(conn)
    }

    // ─── Incoming: iroh → handler ────────────────────────────────────

    fn is_peer_allowed(&self, node_id: &NodeId) -> bool {
        match &self.peer_auth {
            None => true,
            Some(auth) => auth.lock().unwrap().is_allowed(node_id),
        }
    }

    async fn receive_pending(&self) -> (Vec<(String, Vec<u8>, PublicKey)>, Vec<(NodeId, Connection)>) {
        let mut messages = Vec::new();

        // Drain connections accepted by the background accept loop
        let new_connections: Vec<(NodeId, Connection)> = {
            let mut buf = self.accepted_conns.lock().unwrap();
            buf.drain(..).collect()
        };

        // Read streams from newly accepted connections
        for (node_id, conn) in &new_connections {
            let remote_id = PublicKey::from_bytes(&node_id.0).unwrap();
            self.read_streams(conn, remote_id, &mut messages).await;
        }

        // Also read from existing cached connections
        let conn_snapshot: Vec<(NodeId, Connection)> = self
            .connections
            .iter()
            .map(|(id, c)| (*id, c.clone()))
            .collect();

        for (node_id, conn) in conn_snapshot {
            let remote_id = PublicKey::from_bytes(&node_id.0).unwrap();
            self.read_streams(&conn, remote_id, &mut messages).await;
        }

        if !messages.is_empty() {
            eprintln!("iroh driver: received {} message(s)", messages.len());
        }

        (messages, new_connections)
    }

    async fn read_streams(
        &self,
        conn: &Connection,
        remote_id: PublicKey,
        messages: &mut Vec<(String, Vec<u8>, PublicKey)>,
    ) {
        loop {
            match tokio::time::timeout(Duration::from_millis(1), conn.accept_uni()).await {
                Ok(Ok(mut recv)) => {
                    match read_message(&mut recv).await {
                        Ok((tag, payload)) => {
                            messages.push((tag, payload, remote_id));
                        }
                        Err(e) => {
                            eprintln!("iroh driver: read error: {e}");
                            break;
                        }
                    }
                }
                _ => break,
            }
        }
    }

    fn dispatch_incoming(
        &mut self,
        tag: &str,
        payload: &[u8],
        _from: NodeId,
    ) -> Vec<NodeAction> {
        match tag {
            "swactor_dist::Ping" => match serde_json::from_slice::<Ping>(payload) {
                Ok(msg) => self.node.handle_ping(msg.from, msg.sequence, &msg.piggyback),
                Err(e) => {
                    eprintln!("iroh driver: decode Ping: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::Ack" => match serde_json::from_slice::<Ack>(payload) {
                Ok(msg) => self.node.handle_ack(msg.from, msg.sequence, &msg.piggyback),
                Err(e) => {
                    eprintln!("iroh driver: decode Ack: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::PingReq" => match serde_json::from_slice::<PingReq>(payload) {
                Ok(msg) => {
                    self.node
                        .handle_ping_req(msg.from, msg.target, msg.sequence, &msg.piggyback)
                }
                Err(e) => {
                    eprintln!("iroh driver: decode PingReq: {e}");
                    Vec::new()
                }
            },

            "swactor_dist::JoinRequest" => {
                match serde_json::from_slice::<JoinRequest>(payload) {
                    Ok(msg) => self.node.handle_join_request(msg.from),
                    Err(e) => {
                        eprintln!("iroh driver: decode JoinRequest: {e}");
                        Vec::new()
                    }
                }
            }

            "swactor_dist::JoinResponse" => {
                match serde_json::from_slice::<JoinResponse>(payload) {
                    Ok(msg) => self.node.handle_join_response(msg.members),
                    Err(e) => {
                        eprintln!("iroh driver: decode JoinResponse: {e}");
                        Vec::new()
                    }
                }
            }

            "swactor_dist::IndirectAck" => match serde_json::from_slice::<IndirectAck>(payload) {
                Ok(msg) => self.node.handle_indirect_ack(msg.target, msg.sequence, &msg.piggyback),
                Err(e) => {
                    eprintln!("iroh driver: decode IndirectAck: {e}");
                    Vec::new()
                }
            },

            other => {
                eprintln!("iroh driver: unknown message type: {other}");
                Vec::new()
            }
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

    /// Shut down the driver: stop the embedded relay (if any), then close the
    /// iroh endpoint.
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
    let server = iroh_relay::server::Server::spawn(
        iroh_relay::server::ServerConfig::<(), ()> {
            relay: Some(iroh_relay::server::RelayConfig {
                http_bind_addr: bind_addr,
                tls: None,
                limits: Default::default(),
                key_cache_capacity: Some(256),
                access: iroh_relay::server::AccessConfig::Everyone,
            }),
            quic: None,
            metrics_addr: None,
        },
    )
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

/// Extract the send target from a node action (if it has one).
fn action_target(action: &NodeAction) -> Option<NodeId> {
    match action {
        NodeAction::SendPing { to, .. } => Some(*to),
        NodeAction::SendAck { to, .. } => Some(*to),
        NodeAction::SendPingReq { relay, .. } => Some(*relay),
        NodeAction::SendJoinResponse { to, .. } => Some(*to),
        NodeAction::ForwardAck { to, .. } => Some(*to),
        NodeAction::MembershipChanged { .. } => None,
    }
}

// ─── Wire Framing Over QUIC Streams ─────────────────────────────────────────

/// Write a tagged message to a QUIC send stream.
///
/// Frame format: `[4B tag_len][tag_bytes][payload_bytes]`
async fn write_message(
    send: &mut iroh::endpoint::SendStream,
    tag: &[u8],
    payload: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    let tag_len = (tag.len() as u32).to_be_bytes();
    send.write_all(&tag_len).await?;
    send.write_all(tag).await?;
    send.write_all(payload).await?;
    Ok(())
}

/// Read a tagged message from a QUIC recv stream.
///
/// Returns `(type_tag, payload)`.
async fn read_message(
    recv: &mut iroh::endpoint::RecvStream,
) -> Result<(String, Vec<u8>), Box<dyn std::error::Error>> {
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

    Ok((tag, payload))
}
