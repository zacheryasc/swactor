use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use distribution::directory_actor::{DirectoryActor, DirectoryIn};
use distribution::membership_actor::{ObservedMembershipTransition, TransportMembershipActor};
use distribution::messages::actor_codec_registry;
use distribution::node::DistributedNodeConfig;
use distribution::node_metadata_actor::{MetadataActor, MetadataIn};
use distribution::registry_actor::{RegistryActor, RegistryIn};
use distribution::swim::actor::MembershipChanged;
use distribution::transport_bridge::{
    Outbox, OutboxPeerDirectory, OutboxRouteBinder, RelayMirror, RouteView, RouteViewTransport,
};
use distribution::types::{MemberState, NodeId};
use iroh_driver::{ActorBridgeConfig, IrohDriver};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{Runtime, RuntimeConfig, RuntimeParts};
use swactor_transport::{CodecRegistry, CodecRemoteSink, TransportRouter};
use tokio::sync::watch;

pub type Membership = watch::Receiver<HashMap<NodeId, MemberState>>;

pub fn runtime_parts() -> (RuntimeParts, Arc<CodecRegistry>, Arc<TransportRouter>) {
    let parts = RuntimeParts::new(RuntimeConfig::default());
    let codec = Arc::new(actor_codec_registry());
    let router = Arc::new(TransportRouter::new());
    parts.runtime().set_remote_sink(Arc::new(CodecRemoteSink::new(
        codec.clone(),
        router.clone(),
    )));
    (parts, codec, router)
}

pub fn install(
    driver: &mut IrohDriver,
    runtime: Runtime,
    codec: Arc<CodecRegistry>,
    router: Arc<TransportRouter>,
    name: String,
) -> Result<Membership, swactor::Error> {
    let node_id = driver.node_id();
    let config = DistributedNodeConfig::default();
    let outbox: Outbox = Arc::new(Default::default());
    let relay_mirror: RelayMirror = Arc::new(RwLock::new(HashMap::new()));
    let route_view: RouteView = Arc::new(RwLock::new(HashMap::new()));
    let peers = Arc::new(OutboxPeerDirectory::new(router.clone(), outbox.clone()));
    let registry = runtime.spawn(
        RegistryActor::new(node_id, config.registry, peers.clone()).with_event_dissemination(),
    )?;
    let metadata = runtime.spawn(
        MetadataActor::new(node_id, config.metadata_lambda, peers.clone(), relay_mirror.clone())
            .with_event_dissemination(),
    )?;
    let binder = Arc::new(OutboxRouteBinder::new(
        router,
        Arc::new(RouteViewTransport::new(route_view.clone(), outbox.clone())),
    ));
    let directory = runtime.spawn(
        DirectoryActor::new(node_id, peers.clone(), route_view.clone(), binder)
            .with_event_dissemination(),
    )?;
    let (membership_tx, membership) = watch::channel(HashMap::new());
    let observer = runtime.spawn(MembershipFanout {
        registry,
        metadata,
        directory,
        membership: membership_tx,
    })?;
    let swim = runtime.spawn(TransportMembershipActor::new(node_id, peers, observer))?;
    let mut routes = HashMap::new();
    for tag in [
        "swactor_dist::JoinRequest",
        "swactor_dist::JoinResponse",
        "swactor_dist::MembershipGossip",
    ] {
        routes.insert(tag.to_owned(), swim);
    }
    routes.insert("swactor_dist::RegistryGossip".to_owned(), registry);
    routes.insert("swactor_dist::MetadataGossip".to_owned(), metadata);
    for tag in [
        "swactor_dist::DirectoryGossip",
        "swactor_dist::DirectoryRouteOffer",
        "swactor_dist::DirectoryRouteAck",
    ] {
        routes.insert(tag.to_owned(), directory);
    }
    driver.enable_actor_bridge(ActorBridgeConfig {
        runtime: runtime.clone(),
        codec,
        routes,
        swim,
        relay_mirror,
        route_view,
        outbox,
    });
    runtime.send_to(metadata, MetadataIn::SetNodeName { name })?;
    runtime.send_to(metadata, MetadataIn::SetRelayUrl {
        url: driver.relay_url().map(str::to_owned),
    })?;
    driver.install_actor_bridge();
    Ok(membership)
}

struct MembershipFanout {
    registry: ActorAddress,
    metadata: ActorAddress,
    directory: ActorAddress,
    membership: watch::Sender<HashMap<NodeId, MemberState>>,
}

impl ActorInterface for MembershipFanout {
    type Incoming = ObservedMembershipTransition;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, transition: Self::Incoming) {
        self.membership.send_modify(|members| {
            members.insert(transition.peer, transition.to);
        });
        let change = MembershipChanged {
            node_id: transition.peer,
            state: transition.to,
            incarnation: transition.incarnation,
        };
        let _ = ctx.send(self.registry, RegistryIn::Membership(change.clone()));
        let _ = ctx.send(self.metadata, MetadataIn::Membership(change.clone()));
        let _ = ctx.send(self.directory, DirectoryIn::Membership(change));
    }
}
