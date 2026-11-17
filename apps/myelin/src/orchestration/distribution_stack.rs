//! Myelin-system swactor distribution runtime wiring.
//!
//! This is the production version of the actor-stack setup that integration
//! tests used to copy by hand: a swactor runtime, event-driven membership and
//! distribution actors, codec/transport routing, and actor-directory mirrors.
//! Authenticated transport lifecycle and actual mutations drive progression.

use parking_lot::Mutex as ObservationMutex;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex, RwLock, Weak};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::config::RuntimeConfig;
use swactor::runtime::{Ctx, Runtime, RuntimeParts};
use swactor::stats::StatsHook;
use swactor::std::StdExtension;
use swactor_engine::EngineHandle;
use swactor_transport::{CodecRegistry, CodecRemoteSink, NetworkMessage, TransportRouter};

use distribution::directory_actor::{DirectoryActor, DirectoryClaims, DirectoryIn};
use distribution::membership_actor::{ObservedMembershipTransition, TransportMembershipActor};
use distribution::messages::{
    DirectoryGossip, MetadataGossip, RegistryGossip, actor_codec_registry,
};
use distribution::node::DistributedNodeConfig;
use distribution::node_metadata_actor::{MetadataActor, MetadataIn};
use distribution::registry_actor::{RegistryActor, RegistryIn, RegistryView};
use distribution::swim::actor::MembershipChanged;
use distribution::swim::member_list::MemberList;
use distribution::telemetry::TransportMembershipTransition;
use distribution::transport_bridge::{
    Outbox, OutboxPeerDirectory, OutboxRouteBinder, RelayMirror, RouteView, RouteViewTransport,
};
use distribution::types::{DirectoryEntry, MemberState, NodeId};

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
    /// The engine owning this runtime and its supporting I/O work.
    pub engine: EngineHandle,
    pub codec: Arc<CodecRegistry>,
    pub outbox: Outbox,
    pub relay_mirror: RelayMirror,
    pub route_view: RouteView,
    pub pinned_routes: RouteView,
    pub route_binder: Arc<OutboxRouteBinder>,
    pub registry_view: RegistryView,
    pub directory_claims: DirectoryClaims,
    pub membership_mirror: Arc<Mutex<MemberList>>,
    membership_transitions: Arc<ObservationMutex<VecDeque<ObservedMembershipTransition>>>,
    membership_watchers: Arc<ObservationMutex<Vec<Weak<dyn Fn() + Send + Sync>>>>,
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
        let outbox: Outbox = Arc::new(Default::default());
        let relay_mirror: RelayMirror = Arc::new(RwLock::new(HashMap::new()));
        let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
        let pinned_routes: RouteView = Arc::new(RwLock::new(HashMap::new()));
        let peer_directory = Arc::new(OutboxPeerDirectory::new(
            Arc::clone(&transport_router),
            Arc::clone(&outbox),
        ));
        let registry_view = Arc::new(RwLock::new(Default::default()));

        let registry_addr = runtime
            .spawn(
                RegistryActor::new(node_id, config.registry.clone(), peer_directory.clone())
                    .with_view(Arc::clone(&registry_view))
                    .with_event_dissemination(),
            )
            .expect("spawn RegistryActor");
        let metadata_addr = runtime
            .spawn(
                MetadataActor::new(
                    node_id,
                    config.metadata_lambda,
                    peer_directory.clone(),
                    Arc::clone(&relay_mirror),
                )
                .with_event_dissemination(),
            )
            .expect("spawn MetadataActor");
        let route_view_transport = Arc::new(RouteViewTransport::new(
            Arc::clone(&route_view),
            Arc::clone(&outbox),
        ));
        let route_binder = Arc::new(OutboxRouteBinder::new(
            Arc::clone(&transport_router),
            Arc::clone(&route_view_transport),
        ));
        let directory = DirectoryActor::with_pinned_routes(
            node_id,
            peer_directory.clone(),
            Arc::clone(&route_view),
            Arc::clone(&pinned_routes),
            route_binder.clone(),
        )
        .with_event_dissemination();
        let directory_claims = directory.claims();
        let directory_addr = runtime.spawn(directory).expect("spawn DirectoryActor");

        let membership_mirror = Arc::new(Mutex::new(MemberList::new(NodeId([0xFF; 32]))));
        let membership_transitions = Arc::new(ObservationMutex::new(VecDeque::new()));
        let membership_watchers = Arc::new(ObservationMutex::new(Vec::new()));
        let fanout_addr = runtime
            .spawn(MembershipFanout {
                registry: registry_addr,
                metadata: metadata_addr,
                directory: directory_addr,
                mirror: Arc::clone(&membership_mirror),
                transitions: Arc::clone(&membership_transitions),
                watchers: Arc::clone(&membership_watchers),
            })
            .expect("spawn MembershipFanout");
        let swim_addr = runtime
            .spawn(TransportMembershipActor::new(
                node_id,
                peer_directory,
                fanout_addr,
            ))
            .expect("spawn transport membership actor");

        Self {
            runtime,
            engine,
            codec,
            outbox,
            relay_mirror,
            route_view,
            route_binder,
            pinned_routes,
            membership_mirror,
            registry_view,
            directory_claims,
            membership_transitions,
            membership_watchers,
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
            "swactor_dist::MembershipGossip",
        ] {
            routes.insert(tag.to_owned(), self.actors.swim);
        }
        routes.insert(RegistryGossip::type_tag().to_owned(), self.actors.registry);
        routes.insert(MetadataGossip::type_tag().to_owned(), self.actors.metadata);
        routes.insert(
            DirectoryGossip::type_tag().to_owned(),
            self.actors.directory,
        );
        routes.insert(
            "swactor_dist::DirectoryRouteOffer".to_owned(),
            self.actors.directory,
        );
        routes.insert(
            "swactor_dist::DirectoryRouteAck".to_owned(),
            self.actors.directory,
        );
        routes
    }

    /// Observe the published membership mirror. Register before the initial
    /// callback so a concurrent transition cannot be lost.
    pub(crate) fn watch_membership(&self, changed: Weak<dyn Fn() + Send + Sync>) {
        let callback = changed.upgrade();
        self.membership_watchers.lock().push(changed);
        if let Some(callback) = callback {
            callback();
        }
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

    pub(crate) fn drain_membership_transitions(&self) -> Vec<ObservedMembershipTransition> {
        self.membership_transitions.lock().drain(..).collect()
    }

    pub(crate) fn membership_transition(
        &self,
        transition: &ObservedMembershipTransition,
    ) -> TransportMembershipTransition {
        TransportMembershipTransition {
            peer: format!("{:?}", transition.peer),
            from: transition
                .from
                .map(|state| format!("{:?}", state))
                .unwrap_or_default(),
            to: format!("{:?}", transition.to),
            reason: transition.reason.to_owned(),
        }
    }
}

struct MembershipFanout {
    registry: ActorAddress,
    metadata: ActorAddress,
    directory: ActorAddress,
    mirror: Arc<Mutex<MemberList>>,
    transitions: Arc<ObservationMutex<VecDeque<ObservedMembershipTransition>>>,
    watchers: Arc<ObservationMutex<Vec<Weak<dyn Fn() + Send + Sync>>>>,
}

impl ActorInterface for MembershipFanout {
    type Incoming = ObservedMembershipTransition;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, transition: Self::Incoming) {
        let change = MembershipChanged {
            node_id: transition.peer,
            state: transition.to,
            incarnation: transition.incarnation,
        };
        self.mirror
            .lock()
            .expect("membership mirror poisoned")
            .apply(change.node_id, change.state, change.incarnation);
        {
            let mut transitions = self.transitions.lock();
            if transitions.len() == 256 {
                transitions.pop_front();
            }
            transitions.push_back(transition);
        }
        let _ = ctx.send(self.registry, RegistryIn::Membership(change.clone()));
        let _ = ctx.send(self.metadata, MetadataIn::Membership(change.clone()));
        let _ = ctx.send(self.directory, DirectoryIn::Membership(change));
        let mut callbacks = Vec::new();
        self.watchers.lock().retain(|watcher| {
            if let Some(callback) = watcher.upgrade() {
                callbacks.push(callback);
                true
            } else {
                false
            }
        });
        for callback in callbacks {
            callback();
        }
    }
}
