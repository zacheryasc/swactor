//! Myelin-system swactor distribution runtime wiring.
//!
//! This is the production version of the actor-stack setup that integration
//! tests used to copy by hand: a swactor runtime, the four distribution protocol
//! actors, codec/transport routing, and the actor-directory mirrors. Protocol
//! tick injection is owned by the swactor engine (see
//! [`DistributionRuntimeStack::spawn_protocol_ticker`]); the application loop
//! no longer manually ticks core.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::config::RuntimeConfig;
use swactor::runtime::{Ctx, ExternalSender, Runtime, RuntimeParts};
use swactor::stats::StatsHook;
use swactor::std::StdExtension;
use swactor_engine::EngineHandle;
use swactor_transport::{CodecRegistry, CodecRemoteSink, NetworkMessage, TransportRouter};

use distribution::directory_actor::{DirectoryActor, DirectoryIn};
use distribution::messages::{
    DirectoryGossip, MetadataGossip, RegistryGossip, actor_codec_registry,
};
use distribution::node::DistributedNodeConfig;
use distribution::node_metadata_actor::{MetadataActor, MetadataIn};
use distribution::registry_actor::{RegistryActor, RegistryIn, RegistryView};
use distribution::swim::actor::{MembershipChanged, SwimActor, SwimIn};
use distribution::swim::member_list::MemberList;
use distribution::swim::probe::SwimConfig;
use distribution::swim::telemetry::{ObservedProbeEvent, ObservedTransition, SwimTelemetry};
use distribution::telemetry::MembershipTransition;
use distribution::telemetry::SwimProbeEvent;
use distribution::transport_bridge::{
    Outbox, OutboxPeerDirectory, OutboxRouteBinder, RelayMirror, RouteView, RouteViewTransport,
};
use distribution::types::{DirectoryEntry, MemberState, NodeId};

#[derive(Clone)]
struct ProtocolTick;

struct ProtocolTicker {
    runtime: Runtime,
    engine: EngineHandle,
    sender: ExternalSender,
    period: Duration,
    swim: ActorAddress,
    registry: ActorAddress,
    metadata: ActorAddress,
    directory: ActorAddress,
}

impl ProtocolTicker {
    fn schedule(&self, ctx: &Ctx) {
        self.engine.send_after(
            self.period,
            self.sender.clone(),
            ctx.self_addr(),
            ProtocolTick,
        );
    }
}

impl ActorInterface for ProtocolTicker {
    type Incoming = ProtocolTick;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.schedule(ctx);
    }

    fn handle(&mut self, ctx: &Ctx, _message: Self::Incoming) {
        let now = self.engine.now().to_instant();
        let _ = self.runtime.send_to(self.swim, SwimIn::Tick { now });
        let _ = self.runtime.send_to(self.registry, RegistryIn::Tick);
        let _ = self.runtime.send_to(self.metadata, MetadataIn::Tick);
        let _ = self.runtime.send_to(self.directory, DirectoryIn::Tick);
        self.schedule(ctx);
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct DistributionActorAddrs {
    pub swim: ActorAddress,
    pub registry: ActorAddress,
    pub metadata: ActorAddress,
    pub directory: ActorAddress,
    pub membership_fanout: ActorAddress,
}

pub(crate) struct DistributionRuntimeStack {
    pub runtime: Runtime,
    /// The node engine this stack is bound to. Protocol ticking and all
    /// supporting work schedule on this stored handle; the stack does not
    /// accept an unrelated engine at each call (ENGINE_SPEC.md).
    pub engine: EngineHandle,
    pub codec: Arc<CodecRegistry>,
    pub outbox: Outbox,
    pub relay_mirror: RelayMirror,
    pub route_view: RouteView,
    pub registry_view: RegistryView,
    pub membership_mirror: Arc<Mutex<MemberList>>,
    pub swim_telemetry: Arc<SwimTelemetry>,
    pub swim_config: SwimConfig,
    pub actors: DistributionActorAddrs,
}

impl DistributionRuntimeStack {
    /// Build and configure the core swactor runtime parts + codec, returning the
    /// cloned runtime handle and shared transport router needed by
    /// [`new_from_runtime`]. The parts are fully configured — extension, remote
    /// sink, statistics hook — but no actors are spawned yet.
    ///
    /// This split lets the engine own the runtime workers before the driver
    /// exists: construct the parts, clone the runtime handle, hand the parts to
    /// [`Engine::new`](swactor_engine::Engine), create the driver (which needs
    /// the engine handle), then spawn actors via [`new_from_runtime`] using
    /// `driver.node_id()`.
    pub(crate) fn build_runtime(
        extend_codecs: impl FnOnce(&mut CodecRegistry),
        stats_hook: Option<Arc<dyn StatsHook>>,
    ) -> (
        RuntimeParts,
        Runtime,
        Arc<CodecRegistry>,
        Arc<TransportRouter>,
    ) {
        let mut parts = RuntimeParts::new(RuntimeConfig::default())
            .with_extension(Arc::new(StdExtension::new()));
        let mut codec = actor_codec_registry();
        extend_codecs(&mut codec);
        let codec = Arc::new(codec);
        let transport_router = Arc::new(TransportRouter::new());
        let runtime = parts.runtime().clone();
        runtime.set_remote_sink(Arc::new(CodecRemoteSink::new(
            Arc::clone(&codec),
            Arc::clone(&transport_router),
        )));
        if let Some(hook) = stats_hook {
            parts = parts.with_stats_hook(hook);
        }
        (parts, runtime, codec, transport_router)
    }

    /// Spawn the four distribution protocol actors on a pre-built runtime.
    /// Used after [`build_runtime`] when the engine already owns the workers.
    pub(crate) fn new_from_runtime(
        runtime: Runtime,
        codec: Arc<CodecRegistry>,
        transport_router: Arc<TransportRouter>,
        node_id: NodeId,
        config: DistributedNodeConfig,
        engine: EngineHandle,
    ) -> Self {
        let outbox: Outbox = Arc::new(Mutex::new(Vec::new()));
        let relay_mirror: RelayMirror = Arc::new(RwLock::new(HashMap::new()));
        let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
        let peer_directory = Arc::new(OutboxPeerDirectory::new(
            Arc::clone(&transport_router),
            Arc::clone(&outbox),
        ));
        let swim_config = config.swim.clone();
        let swim_telemetry = SwimTelemetry::new();
        let registry_view = Arc::new(RwLock::new(Default::default()));

        let swim_addr = runtime
            .spawn(
                SwimActor::new(
                    node_id,
                    swim_config.clone(),
                    Instant::now(),
                    peer_directory.clone(),
                )
                .with_observer(Box::new(Arc::clone(&swim_telemetry))),
            )
            .expect("spawn SwimActor");
        let registry_addr = runtime
            .spawn(
                RegistryActor::new(node_id, config.registry.clone(), peer_directory.clone())
                    .with_view(Arc::clone(&registry_view)),
            )
            .expect("spawn RegistryActor");
        let metadata_addr = runtime
            .spawn(MetadataActor::new(
                node_id,
                config.metadata_lambda,
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
        let directory_addr = runtime
            .spawn(DirectoryActor::new(
                node_id,
                peer_directory,
                Arc::clone(&route_view),
                route_binder,
            ))
            .expect("spawn DirectoryActor");

        let membership_mirror = Arc::new(Mutex::new(MemberList::new(NodeId([0xFF; 32]))));
        let fanout_addr = runtime
            .spawn(MembershipFanout {
                registry: registry_addr,
                metadata: metadata_addr,
                directory: directory_addr,
                mirror: Arc::clone(&membership_mirror),
            })
            .expect("spawn MembershipFanout");
        runtime
            .send_to(
                swim_addr,
                SwimIn::Subscribe {
                    observer: fanout_addr,
                },
            )
            .expect("subscribe MembershipFanout");

        Self {
            runtime,
            engine,
            codec,
            outbox,
            relay_mirror,
            route_view,
            membership_mirror,
            registry_view,
            swim_telemetry,
            swim_config,
            actors: DistributionActorAddrs {
                swim: swim_addr,
                registry: registry_addr,
                metadata: metadata_addr,
                directory: directory_addr,
                membership_fanout: fanout_addr,
            },
        }
    }

    pub(crate) fn actor_bridge_routes(&self) -> HashMap<String, ActorAddress> {
        let mut routes = HashMap::new();
        for tag in [
            "swactor_dist::Ping",
            "swactor_dist::Ack",
            "swactor_dist::PingReq",
            "swactor_dist::IndirectAck",
            "swactor_dist::JoinRequest",
            "swactor_dist::JoinResponse",
        ] {
            routes.insert(tag.to_owned(), self.actors.swim);
        }
        routes.insert(RegistryGossip::type_tag().to_owned(), self.actors.registry);
        routes.insert(MetadataGossip::type_tag().to_owned(), self.actors.metadata);
        routes.insert(
            DirectoryGossip::type_tag().to_owned(),
            self.actors.directory,
        );
        routes
    }

    /// Spawn the actor that owns periodic distribution protocol ticks.
    pub(crate) fn spawn_protocol_ticker(&self, period: Duration) {
        self.runtime
            .spawn(ProtocolTicker {
                runtime: self.runtime.clone(),
                engine: self.engine.clone(),
                sender: self.runtime.create_sender(),
                period,
                swim: self.actors.swim,
                registry: self.actors.registry,
                metadata: self.actors.metadata,
                directory: self.actors.directory,
            })
            .expect("spawn distribution protocol ticker actor");
    }

    pub(crate) fn register_local_actor(&self, entry: DirectoryEntry) {
        let _ = self
            .runtime
            .send_to(self.actors.directory, DirectoryIn::Register(entry));
    }

    pub(crate) fn member_state(&self, node_id: NodeId) -> Option<MemberState> {
        self.membership_mirror
            .lock()
            .ok()?
            .get(&node_id)
            .map(|entry| entry.state)
    }

    pub(crate) fn route_owner(&self, actor: ActorAddress) -> Option<NodeId> {
        self.route_view.read().ok()?.get(&actor).copied()
    }

    pub(crate) fn drain_swim_transitions(&self) -> Vec<ObservedTransition> {
        self.swim_telemetry.drain_transitions()
    }

    pub(crate) fn drain_swim_probe_events(&self) -> Vec<ObservedProbeEvent> {
        self.swim_telemetry.drain_probe_events()
    }

    pub(crate) fn swim_recent_probe_targets(&self) -> Vec<String> {
        self.swim_telemetry
            .recent_targets()
            .into_iter()
            .map(|node_id| format!("{:?}", node_id))
            .collect()
    }

    pub(crate) fn swim_probe_event_record(
        &self,
        event: ObservedProbeEvent,
        local_phase: &str,
    ) -> SwimProbeEvent {
        let config = &self.swim_config;
        let budget_ms = event.budget_ms;
        SwimProbeEvent {
            event: event.event.to_owned(),
            target: format!("{:?}", event.target),
            sequence: event.sequence,
            kind: event.kind.to_owned(),
            rtt_ms: event.rtt_ms,
            budget_ms,
            budget_ticks: budget_ms,
            last_ack_age_ms: event.last_ack_age.map(duration_ms_u64),
            consecutive_timeouts: event.consecutive_timeouts,
            recent_probe_targets: self.swim_recent_probe_targets(),
            member_state: self
                .member_state(event.target)
                .map(|state| format!("{:?}", state)),
            local_phase: local_phase.to_owned(),
            probe_interval_ms: duration_ms_u64(config.probe_interval),
            probe_timeout_ms: duration_ms_u64(config.probe_timeout),
            indirect_probes: u32::try_from(config.indirect_probes).unwrap_or(u32::MAX),
            suspicion_timeout_ms: duration_ms_u64(config.suspicion_timeout),
            dead_reprobe_interval_ms: duration_ms_u64(config.dead_reprobe_interval),
            probe_mode: format!("{:?}", config.probe_mode),
            lifeguard_enabled: config.lifeguard.is_some(),
        }
    }
    pub(crate) fn membership_transition(
        &self,
        transition: &ObservedTransition,
    ) -> MembershipTransition {
        MembershipTransition {
            peer: format!("{:?}", transition.peer),
            from: transition
                .from
                .map(|state| format!("{:?}", state))
                .unwrap_or_default(),
            to: format!("{:?}", transition.to),
            reason: transition.reason.to_owned(),
            last_ack_age_ms: transition.last_ack_age.map(duration_ms_u64),
            consecutive_timeouts: transition.consecutive_timeouts,
            recent_probe_targets: self.swim_recent_probe_targets(),
            member_state: self
                .member_state(transition.peer)
                .map(|state| format!("{:?}", state)),
        }
    }
}

pub(crate) fn duration_ms_u64(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

struct MembershipFanout {
    registry: ActorAddress,
    metadata: ActorAddress,
    directory: ActorAddress,
    mirror: Arc<Mutex<MemberList>>,
}

impl ActorInterface for MembershipFanout {
    type Incoming = MembershipChanged;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, change: Self::Incoming) {
        self.mirror
            .lock()
            .expect("membership mirror poisoned")
            .apply(change.node_id, change.state, change.incarnation);
        let _ = ctx.send(self.registry, RegistryIn::Membership(change.clone()));
        let _ = ctx.send(self.metadata, MetadataIn::Membership(change.clone()));
        let _ = ctx.send(self.directory, DirectoryIn::Membership(change));
    }
}
