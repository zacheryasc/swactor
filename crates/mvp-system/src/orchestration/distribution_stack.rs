#![allow(dead_code)]

//! MVP-system swactor distribution runtime wiring.
//!
//! This is the production version of the actor-stack setup that integration
//! tests used to copy by hand: a swactor runtime, the four distribution protocol
//! actors, codec/transport routing, the actor-directory mirrors, and one pumpable
//! tick seam for concrete network drivers such as `iroh-driver`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::config::RuntimeConfig;
use swactor::runtime::{Ctx, Runtime};
use swactor::std::StdExtension;
use swactor_transport::{CodecRegistry, CodecRemoteSink, NetworkMessage, TransportRouter};

use distribution::directory_actor::{DirectoryActor, DirectoryIn};
use distribution::messages::{
    DirectoryGossip, MetadataGossip, RegistryGossip, actor_codec_registry,
};
use distribution::node::DistributedNodeConfig;
use distribution::node_metadata_actor::{MetadataActor, MetadataIn};
use distribution::registry_actor::{RegistryActor, RegistryIn};
use distribution::swim::actor::{MembershipChanged, SwimActor, SwimIn};
use distribution::swim::member_list::MemberList;
use distribution::swim::probe::SwimConfig;
use distribution::swim::telemetry::{ObservedProbeEvent, ObservedTransition, SwimTelemetry};
use distribution::transport_bridge::{
    Outbox, OutboxPeerDirectory, OutboxRouteBinder, RelayMirror, RouteView, RouteViewTransport,
};
use distribution::types::{DirectoryEntry, MemberState, NodeId};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DistributionActorAddrs {
    pub swim: ActorAddress,
    pub registry: ActorAddress,
    pub metadata: ActorAddress,
    pub directory: ActorAddress,
    pub membership_fanout: ActorAddress,
}

pub struct DistributionRuntimeStack {
    pub node_id: NodeId,
    pub runtime: Arc<Runtime>,
    pub codec: Arc<CodecRegistry>,
    pub outbox: Outbox,
    pub relay_mirror: RelayMirror,
    pub route_view: RouteView,
    pub membership_mirror: Arc<Mutex<MemberList>>,
    pub swim_telemetry: Arc<SwimTelemetry>,
    pub swim_config: SwimConfig,
    pub actors: DistributionActorAddrs,
}

impl DistributionRuntimeStack {
    pub fn new(node_id: NodeId, config: DistributedNodeConfig) -> Self {
        Self::new_with_codecs(node_id, config, |_| {})
    }

    pub fn new_with_codecs(
        node_id: NodeId,
        config: DistributedNodeConfig,
        extend_codecs: impl FnOnce(&mut CodecRegistry),
    ) -> Self {
        let mut runtime =
            Runtime::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()));
        let mut codec = actor_codec_registry();
        extend_codecs(&mut codec);
        let codec = Arc::new(codec);
        let transport_router = Arc::new(TransportRouter::new());
        runtime.set_remote_sink(Arc::new(CodecRemoteSink::new(
            Arc::clone(&codec),
            Arc::clone(&transport_router),
        )));
        let runtime = Arc::new(runtime);

        let outbox: Outbox = Arc::new(Mutex::new(Vec::new()));
        let relay_mirror: RelayMirror = Arc::new(RwLock::new(HashMap::new()));
        let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
        let peer_directory = Arc::new(OutboxPeerDirectory::new(
            Arc::clone(&transport_router),
            Arc::clone(&outbox),
        ));
        let swim_config = config.swim.clone();
        let swim_telemetry = SwimTelemetry::new();

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
            .spawn(RegistryActor::new(
                node_id,
                config.registry.clone(),
                peer_directory.clone(),
            ))
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
            node_id,
            runtime,
            codec,
            outbox,
            relay_mirror,
            route_view,
            membership_mirror,
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

    pub fn actor_bridge_routes(&self) -> HashMap<String, ActorAddress> {
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

    pub fn tick_protocol_actors(&self, now: Instant) {
        let _ = self.runtime.send_to(self.actors.swim, SwimIn::Tick { now });
        let _ = self.runtime.send_to(self.actors.registry, RegistryIn::Tick);
        let _ = self.runtime.send_to(self.actors.metadata, MetadataIn::Tick);
        let _ = self
            .runtime
            .send_to(self.actors.directory, DirectoryIn::Tick);
    }

    pub fn pump_runtime_once(&self) {
        self.runtime.tick();
    }

    pub fn register_local_actor(&self, entry: DirectoryEntry) {
        let _ = self
            .runtime
            .send_to(self.actors.directory, DirectoryIn::Register(entry));
    }

    pub fn alive_count(&self) -> usize {
        self.membership_mirror
            .lock()
            .expect("membership mirror poisoned")
            .all_members()
            .iter()
            .filter(|entry| entry.state == MemberState::Alive)
            .count()
    }

    pub fn member_state(&self, node_id: NodeId) -> Option<MemberState> {
        self.membership_mirror
            .lock()
            .ok()?
            .get(&node_id)
            .map(|entry| entry.state)
    }

    pub fn route_owner(&self, actor: ActorAddress) -> Option<NodeId> {
        self.route_view.read().ok()?.get(&actor).copied()
    }

    pub fn drain_swim_transitions(&self) -> Vec<ObservedTransition> {
        self.swim_telemetry.drain_transitions()
    }

    pub fn drain_swim_probe_events(&self) -> Vec<ObservedProbeEvent> {
        self.swim_telemetry.drain_probe_events()
    }
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
