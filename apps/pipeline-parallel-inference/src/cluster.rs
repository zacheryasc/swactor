//! `ClusterNode` — the actorized distribution protocol bundled behind a
//! synchronous façade.
//!
//! Mirrors the actorized test fixture at `crates/distribution/tests/common/iroh.rs`.
//! Every old
//! `driver.node()` / `driver.node_mut()` / `driver.recv()` / `driver.tick()`
//! call site in the pp binaries and tests rewrites to one method on this type.
//!
//! Layout (one node):
//!
//! * `IrohDriver` — transport bridge only (decode inbound → mailbox; drain
//!   outbox → iroh writes). No protocol state.
//! * A per-node swactor `Runtime` hosting the four protocol actors —
//!   `SwimActor`, `RegistryActor`, `MetadataActor`, `DirectoryActor` — plus a
//!   `MembershipFanout` that adapts `SwimIn::MembershipChanged` into the other
//!   three actors and folds it into a shared `MemberList` mirror.
//! * Egress glue: a shared `Outbox`, `RelayMirror`, `RouteView`, and an
//!   `OutboxPeerDirectory` so the actors can resolve a `NodeId` to a route at
//!   send time and the driver can pick up frames at drain time.
//!
//! Synchronous façade — see [`ClusterNode::register_name`],
//! [`ClusterNode::resolve_name`], [`ClusterNode::set_relay_url`],
//! [`ClusterNode::peer_relay_url`], [`ClusterNode::pump_once`],
//! [`ClusterNode::snapshot`], [`ClusterNode::alive_count`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use iroh::EndpointAddr;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::config::RuntimeConfig;
use swactor::runtime::{Ctx, Inbox, Runtime};
use swactor::std::StdExtension;
use swactor_transport::{CodecRegistry, TransportRouter};

use distribution::directory_actor::{DirectoryActor, DirectoryIn};
use distribution::node::DistributedNodeConfig;
use distribution::node_metadata_actor::{MetadataActor, MetadataIn, RelayInfo};
use distribution::registry::RegistrySnapshot;
use distribution::registry_actor::{NameResolved, RegistryActor, RegistryIn, RegistryView};
use distribution::snapshot::{
    CacheEntryInfo, DistributionNodeSnapshot, MemberInfo, RegistryEntryInfo,
};
use distribution::swim::actor::{MembershipChanged, SwimActor, SwimIn};
use distribution::swim::member_list::MemberList;
use distribution::swim::telemetry::{ObservedTransition, SwimTelemetry};
use distribution::transport_bridge::{
    Outbox, OutboxPeerDirectory, OutboxRouteBinder, RelayMirror, RouteView, RouteViewTransport,
};
use distribution::types::{MemberState, NodeId};
use iroh_driver::{IrohDriver, IrohDriverConfig};

/// Adapts the `SwimActor`'s `MembershipChanged` stream into the registry /
/// metadata / directory actors' `Membership` control messages and folds it
/// into a shared `MemberList` mirror — the same source production's snapshot
/// uses for its members list.
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
        self.mirror
            .lock()
            .unwrap()
            .apply(m.node_id, m.state, m.incarnation);
        let _ = ctx.send(self.registry, RegistryIn::Membership(m.clone()));
        let _ = ctx.send(self.metadata, MetadataIn::Membership(m.clone()));
        let _ = ctx.send(self.directory, DirectoryIn::Membership(m));
    }
}

/// One bundled node: driver + swactor runtime + four protocol actors + the
/// membership fanout, with a synchronous façade for the pp binaries and tests.
pub struct ClusterNode {
    pub driver: IrohDriver,
    pub rt: Arc<Runtime>,
    pub transport_router: Arc<TransportRouter>,
    pub codecs: Arc<CodecRegistry>,
    outbox: Outbox,
    swim_addr: ActorAddress,
    registry_addr: ActorAddress,
    metadata_addr: ActorAddress,
    directory_addr: ActorAddress,
    membership_mirror: Arc<Mutex<MemberList>>,
    relay_mirror: RelayMirror,
    _route_view: RouteView,
    /// Read-mirror of the cluster registry, for the `dist.state` telemetry.
    registry_view: RegistryView,
    /// Production SWIM observer: probe RTT, recent probe targets, and membership
    /// transitions (with cause), for `transport.internals` / `membership` / `dist.state`.
    swim_telemetry: Arc<SwimTelemetry>,
    /// Reused per `resolve_name` call to avoid leaking inbox addresses in the
    /// runtime's inbox registry.
    resolve_inbox: Inbox<NameResolved>,
    /// Reused per `peer_relay_url` call for the same reason.
    relay_inbox: Inbox<RelayInfo>,
}

impl ClusterNode {
    /// Build a node from a driver config + the codec registry the binary will
    /// use for app traffic. The codec registry **must** already include the
    /// distribution protocol types — see `inference_codec_registry` (which
    /// composes them) or `distribution::messages::actor_codec_registry`.
    ///
    /// Constructs the iroh endpoint on the *ambient* tokio handle (i.e. the
    /// caller is `fn main()` running outside of `#[tokio::main]`). For tests
    /// running on a shared tokio runtime, use [`ClusterNode::with_handle`].
    ///
    /// `customize_rt` runs against the bare swactor runtime BEFORE it is
    /// wrapped in `Arc`. Use this to install a `StatsHook` (the dashboard
    /// stats collector), set any extensions, etc. — anything that requires
    /// `&mut Runtime`. Pass `|_| {}` if nothing extra is needed.
    pub fn new(
        config: IrohDriverConfig,
        node_config: DistributedNodeConfig,
        codecs: CodecRegistry,
        customize_rt: impl FnOnce(&mut Runtime),
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let driver = IrohDriver::new(config)?;
        Ok(Self::wire(driver, node_config, codecs, customize_rt))
    }

    /// Build a node from a driver config on the supplied tokio handle. Sync
    /// `#[test]`s that share one global tokio runtime should use this.
    pub fn with_handle(
        handle: tokio::runtime::Handle,
        config: IrohDriverConfig,
        node_config: DistributedNodeConfig,
        codecs: CodecRegistry,
        customize_rt: impl FnOnce(&mut Runtime),
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let driver = IrohDriver::with_handle(handle, config)?;
        Ok(Self::wire(driver, node_config, codecs, customize_rt))
    }

    fn wire(
        mut driver: IrohDriver,
        node_config: DistributedNodeConfig,
        codecs: CodecRegistry,
        customize_rt: impl FnOnce(&mut Runtime),
    ) -> Self {
        let node_id = driver.node_id();
        let swim_config = node_config.swim.clone();
        let registry_config = node_config.registry.clone();
        let metadata_lambda = node_config.metadata_lambda;

        // Per-node swactor runtime + codec + transport router. The same router
        // is used both for protocol-actor egress (the codec covers the SWIM /
        // gossip wire tags) and for app-actor egress (the codec the caller
        // passed in covers the pp wire tags).
        let mut swactor_rt =
            Runtime::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()));
        let codec_registry = Arc::new(codecs);
        let transport_router = Arc::new(TransportRouter::new());
        swactor_rt.set_remote_sink(Arc::new(swactor_transport::CodecRemoteSink::new(
            Arc::clone(&codec_registry),
            Arc::clone(&transport_router),
        )));
        customize_rt(&mut swactor_rt);
        let rt: Arc<Runtime> = Arc::new(swactor_rt);

        // Shared egress state.
        let outbox: Outbox = Arc::new(Mutex::new(Vec::new()));
        let relay_mirror: RelayMirror = Arc::new(RwLock::new(HashMap::new()));
        let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
        let peer_directory = Arc::new(OutboxPeerDirectory::new(
            Arc::clone(&transport_router),
            Arc::clone(&outbox),
        ));

        // Telemetry mirrors observed by the fleet emitter (same wiring as the
        // standalone node): the SWIM observer (probe RTT, recent targets,
        // transitions-with-cause) and the registry read-mirror.
        let swim_telemetry = SwimTelemetry::new();
        let registry_view: RegistryView = Arc::new(RwLock::new(RegistrySnapshot::default()));

        // The four protocol actors.
        let swim_addr = rt
            .spawn(
                SwimActor::new(node_id, swim_config, Instant::now(), peer_directory.clone())
                    .with_observer(Box::new(Arc::clone(&swim_telemetry))),
            )
            .expect("spawn SwimActor");
        let registry_addr = rt
            .spawn(
                RegistryActor::new(node_id, registry_config, peer_directory.clone())
                    .with_view(Arc::clone(&registry_view)),
            )
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

        // Membership fanout adapts SwimIn::MembershipChanged into the other
        // actors' control mailboxes and folds into the mirror. The mirror's
        // sentinel self-id ensures it stores every real peer (a MemberList
        // never stores its own id).
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

        // Ingress: which local actor owns each inbound wire tag.
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
            Arc::clone(&rt),
            Arc::clone(&codec_registry),
            routes,
            swim_addr,
            Arc::clone(&relay_mirror),
            Arc::clone(&route_view),
        );

        let resolve_inbox: Inbox<NameResolved> = rt.new_inbox().expect("alloc resolve_name inbox");
        let relay_inbox: Inbox<RelayInfo> = rt.new_inbox().expect("alloc relay_lookup inbox");

        Self {
            driver,
            rt,
            transport_router,
            codecs: codec_registry,
            outbox,
            swim_addr,
            registry_addr,
            metadata_addr,
            directory_addr,
            membership_mirror,
            relay_mirror,
            _route_view: route_view,
            registry_view,
            swim_telemetry,
            resolve_inbox,
            relay_inbox,
        }
    }

    // ── Telemetry accessors (for the fleet emitter) ─────────────────────────

    /// A consistent snapshot of the cluster registry (size / tombstones / entries).
    pub fn registry_snapshot(&self) -> RegistrySnapshot {
        self.registry_view
            .read()
            .expect("registry view poisoned")
            .clone()
    }

    /// Median SWIM probe round-trip time (ms); `0` until a probe completes.
    pub fn swim_rtt_p50(&self) -> u32 {
        self.swim_telemetry.rtt_ms_p50()
    }

    /// Recent SWIM probe targets (most recent last).
    pub fn swim_recent_targets(&self) -> Vec<NodeId> {
        self.swim_telemetry.recent_targets()
    }

    /// Drain the membership transitions captured since the last call (each carries
    /// a real cause string).
    pub fn drain_swim_transitions(&self) -> Vec<ObservedTransition> {
        self.swim_telemetry.drain_transitions()
    }

    /// This node's location cache: the remote `(actor, host)` pairs it knows, from
    /// the directory route-view **and** the registry's live bindings (the demo
    /// resolves neighbours by name, so the registry is its location directory).
    /// Self-hosted and tombstoned entries are excluded; deduplicated by address.
    pub fn location_cache_entries(&self) -> Vec<(ActorAddress, NodeId)> {
        let self_id = self.driver.node_id();
        let mut seen = std::collections::HashSet::new();
        let mut out: Vec<(ActorAddress, NodeId)> = Vec::new();
        for (addr, host) in self.driver.location_cache_entries() {
            if seen.insert(addr) {
                out.push((addr, host));
            }
        }
        let registry = self.registry_view.read().expect("registry view poisoned");
        for e in &registry.entries {
            if !e.tombstone && e.node_id != self_id && seen.insert(e.actor_addr) {
                out.push((e.actor_addr, e.node_id));
            }
        }
        out.sort_by(|a, b| a.0.0.cmp(&b.0.0));
        out
    }

    // ── Driver passthroughs ─────────────────────────────────────────────────

    pub fn node_id(&self) -> NodeId {
        self.driver.node_id()
    }

    pub fn endpoint_addr(&self) -> EndpointAddr {
        self.driver.endpoint_addr()
    }

    pub fn home_relay_url(&self) -> Option<iroh::RelayUrl> {
        self.driver.home_relay_url()
    }

    pub fn join(&mut self, seeds: &[EndpointAddr]) {
        self.driver.join(seeds)
    }

    // ── The synchronous pump (one full actor-stack step) ────────────────────

    /// One pump iteration. Replaces every `driver.recv(); driver.tick();` +
    /// `rt.tick();` triple in the old code.
    ///
    /// Unconditionally injects the four protocol `Tick`s every call — callers
    /// rate-limit via their own sleep cadence (binaries: 20–100ms; tests:
    /// ~10ms). SWIM is wall-clock driven from `Instant::now()` so the gossip
    /// timers advance regardless of pump frequency.
    pub fn pump_once(&mut self) {
        let now = Instant::now();
        let _ = self.rt.send_to(self.swim_addr, SwimIn::Tick { now });
        let _ = self.rt.send_to(self.registry_addr, RegistryIn::Tick);
        let _ = self.rt.send_to(self.metadata_addr, MetadataIn::Tick);
        let _ = self.rt.send_to(self.directory_addr, DirectoryIn::Tick);
        self.driver.pump_inbound_to_actors();
        self.rt.tick();
        self.driver.drain_outbox(&self.outbox);
    }

    // ── Registry façade ─────────────────────────────────────────────────────

    /// Cluster-wide name registration. Fire-and-forget — the binding is
    /// installed locally on the next `rt.tick()` and gossiped to peers on the
    /// next `RegistryIn::Tick`.
    pub fn register_name(&self, name: &str, addr: ActorAddress) {
        let _ = self.rt.send_to(
            self.registry_addr,
            RegistryIn::RegisterName {
                name: name.to_string(),
                actor_addr: addr,
            },
        );
    }
    pub fn unregister_name(&self, name: &str) {
        let _ = self.rt.send_to(
            self.registry_addr,
            RegistryIn::UnregisterName {
                name: name.to_string(),
            },
        );
    }

    /// Synchronous name resolve. Sends `ResolveName` with the reusable
    /// `resolve_inbox` as the reply target, runs one `rt.tick()` to let the
    /// `RegistryActor` produce the reply, then returns the current binding (or
    /// `None` if the name is not yet known locally).
    ///
    /// Callers should drive gossip convergence by pumping the cluster
    /// (`pump_once`) in a loop with a small sleep between attempts.
    pub fn resolve_name(&self, name: &str) -> Option<(ActorAddress, NodeId)> {
        if self
            .rt
            .send_to(
                self.registry_addr,
                RegistryIn::ResolveName {
                    name: name.to_string(),
                    reply: *self.resolve_inbox.addr(),
                },
            )
            .is_err()
        {
            return None;
        }
        self.rt.tick();
        // Drain any prior stale reply that may have piled up if the caller
        // never consumed it, keeping only the freshest one.
        let mut latest = None;
        while let Some(r) = self.resolve_inbox.try_recv() {
            if r.name == name {
                latest = Some(r);
            }
        }
        latest.and_then(|r| r.binding)
    }

    // ── Metadata façade ─────────────────────────────────────────────────────

    /// Publish this node's home relay URL into the cluster's metadata gossip.
    pub fn set_relay_url(&self, url: Option<String>) {
        let _ = self
            .rt
            .send_to(self.metadata_addr, MetadataIn::SetRelayUrl { url });
    }

    /// Look up a peer's relay URL via the local `relay_mirror`, which the
    /// `MetadataActor` keeps in sync with received gossip.
    pub fn peer_relay_url(&self, peer: NodeId) -> Option<String> {
        // Fast path: the mirror is what the driver itself reads when dialing,
        // and it's already kept up-to-date by metadata gossip.
        if let Ok(m) = self.relay_mirror.read() {
            if let Some(url) = m.get(&peer).cloned() {
                return Some(url);
            }
        }
        // Slow path: ask the actor explicitly (covers the case where a future
        // change moves the source of truth off the mirror).
        if self
            .rt
            .send_to(
                self.metadata_addr,
                MetadataIn::RelayLookup {
                    node: peer,
                    reply: *self.relay_inbox.addr(),
                },
            )
            .is_err()
        {
            return None;
        }
        self.rt.tick();
        let mut latest = None;
        while let Some(r) = self.relay_inbox.try_recv() {
            if r.node == peer {
                latest = Some(r);
            }
        }
        latest.and_then(|r| r.relay_url)
    }

    // ── Membership façade ───────────────────────────────────────────────────

    pub fn sees_alive(&self, peer: &NodeId) -> bool {
        self.membership_mirror
            .lock()
            .unwrap()
            .get(peer)
            .map(|e| e.state == MemberState::Alive)
            .unwrap_or(false)
    }

    pub fn alive_count(&self) -> usize {
        self.membership_mirror.lock().unwrap().alive_count()
    }

    /// All members the SWIM mirror currently knows. The vector is sorted by
    /// `NodeId` and excludes self (the mirror never stores self).
    pub fn members_raw(&self) -> Vec<distribution::swim::member_list::MemberEntry> {
        self.membership_mirror
            .lock()
            .unwrap()
            .all_members()
            .into_iter()
            .cloned()
            .collect()
    }

    /// Build a fully-enriched [`DistributionNodeSnapshot`], folding members
    /// from the SWIM mirror and per-peer relay URLs from the metadata mirror.
    /// Replaces every old `driver.snapshot()` call site that reads `.members`,
    /// `.alive_count`, `.relay_url`, etc.
    ///
    /// Driver-owned fields (`listen_addr`, `directory_route_count`) are filled
    /// from the driver's own snapshot. Registry / cache entry lists are left
    /// empty — they used to come from `DistributedNode`'s internal state and
    /// would require dedicated dump messages on the actors to surface again.
    /// The pp dashboards don't currently read them; revisit if that changes.
    pub fn snapshot(&self) -> DistributionNodeSnapshot {
        let mut snap = self.driver.snapshot();

        // Members + counts from the SWIM mirror, each labelled with the cause of
        // its most recent liveness transition from the production SWIM observer
        // (a non-draining read — the fleet emitter still owns the drain).
        let mirror = self.membership_mirror.lock().unwrap();
        let relays = self.relay_mirror.read().ok();
        let reasons = self.swim_telemetry.last_reasons();
        let mut members: Vec<MemberInfo> = Vec::with_capacity(mirror.len());
        let (mut alive, mut suspect, mut dead) = (0usize, 0usize, 0usize);
        for entry in mirror.all_members() {
            match entry.state {
                MemberState::Alive => alive += 1,
                MemberState::Suspect => suspect += 1,
                MemberState::Dead => dead += 1,
            }
            let node_hex: String = entry
                .node_id
                .0
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect();
            let state = match entry.state {
                MemberState::Alive => "alive",
                MemberState::Suspect => "suspect",
                MemberState::Dead => "dead",
            }
            .to_string();
            let relay_url = relays.as_ref().and_then(|m| m.get(&entry.node_id).cloned());
            members.push(MemberInfo {
                node_id: node_hex,
                addr: None,
                state,
                incarnation: entry.incarnation,
                is_authorized: None,
                label: None,
                relay_url,
                node_name: None,
                reason: reasons.get(&entry.node_id).map(|r| r.to_string()),
            });
        }
        snap.members = members;
        snap.alive_count = alive;
        snap.suspect_count = suspect;
        snap.dead_count = dead;

        // Self relay URL, if any. The driver fills this in `driver.snapshot()`
        // only when it has a `relay_url` of its own; otherwise we surface
        // whatever the metadata actor has gossipped (which, for the local
        // node, is what the binary published via `set_relay_url`).
        if snap.relay_url.is_none() {
            if let Some(r) = self.driver.home_relay_url() {
                snap.relay_url = Some(r.to_string());
            }
        }

        // Registry (name directory), location cache, and recent probe targets,
        // from this node's telemetry mirrors — the `dist.state` fields the
        // Distribution page renders. (`driver.snapshot()` fills only the
        // directory route count.)
        let hex = |bytes: &[u8]| -> String { bytes.iter().map(|b| format!("{b:02x}")).collect() };
        let registry = self.registry_snapshot();
        snap.registry_size = registry.size;
        snap.registry_tombstones = registry.tombstones;
        snap.registry_entries = registry
            .entries
            .iter()
            .map(|e| RegistryEntryInfo {
                name: e.name.clone(),
                actor_addr: hex(&e.actor_addr.0),
                node_id: hex(&e.node_id.0),
                tombstone: e.tombstone,
            })
            .collect();
        let cache = self.location_cache_entries();
        snap.cache_size = cache.len();
        snap.cache_entries = cache
            .iter()
            .map(|(addr, host)| CacheEntryInfo {
                actor_addr: hex(&addr.0),
                node_id: hex(&host.0),
            })
            .collect();
        snap.recent_probe_targets = self
            .swim_recent_targets()
            .iter()
            .map(|t| hex(&t.0))
            .collect();

        snap
    }
}
