//! Directory route-binding lifecycle: republish must unbind actors whose host
//! left the cluster, or the binder and transport router grow without bound
//! under membership churn.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use swactor::actor::ActorAddress;
use swactor::runtime::{RuntimeConfig, RuntimeParts};
use swactor::std::StdExtension;
use swactor_engine::{Engine, SteppingBackend};

use distribution::crypto::{Keypair, KeypairExt};
use distribution::directory_actor::{DirectoryActor, DirectoryIn};
use distribution::swim::actor::{MembershipChanged, SharedPeerDirectory};
use distribution::transport_bridge::{RouteBinder, RouteView};
use distribution::types::{MemberState, NodeId};

/// Records every bind/unbind so a test can assert the republish diff.
struct RecordingBinder {
    events: Mutex<Vec<(ActorAddress, bool)>>,
}

impl RouteBinder for RecordingBinder {
    fn ensure_routable(&self, actor: ActorAddress) {
        self.events.lock().push((actor, true));
    }

    fn remove_route(&self, actor: &ActorAddress) {
        self.events.lock().push((*actor, false));
    }
}

fn membership(node: NodeId, state: MemberState) -> DirectoryIn {
    DirectoryIn::Membership(MembershipChanged {
        node_id: node,
        state,
        incarnation: 0,
    })
}

#[test]
fn republish_unbinds_routes_of_departed_hosts() {
    let parts =
        RuntimeParts::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()));
    let rt = parts.runtime().clone();
    let backend = SteppingBackend::new();
    let _engine = Engine::new(parts, backend.clone()).expect("create engine");

    let self_key = Keypair::generate();
    let self_id = self_key.node_id();
    let peer_key = Keypair::generate();
    let peer_id = peer_key.node_id();

    let route_view: RouteView = Arc::new(std::sync::RwLock::new(HashMap::new()));
    let binder = Arc::new(RecordingBinder {
        events: Mutex::new(Vec::new()),
    });
    let directory = rt
        .spawn(DirectoryActor::new(
            self_id,
            Arc::new(SharedPeerDirectory::new()),
            route_view.clone(),
            binder.clone(),
        ))
        .expect("spawn DirectoryActor");

    // A remotely-hosted actor claim plus its host being alive.
    let actor = ActorAddress::new_random();
    let claim = peer_key.sign_directory_entry(actor, 1);
    rt.send_to(directory, DirectoryIn::Register(claim)).unwrap();
    rt.send_to(directory, membership(peer_id, MemberState::Alive))
        .unwrap();
    backend.step();

    let events = binder.events.lock().clone();
    assert!(
        events.contains(&(actor, true)),
        "alive peer's actor must be bound: {events:?}"
    );

    // Host dies: its actor leaves the route view and must be unbound.
    rt.send_to(directory, membership(peer_id, MemberState::Dead))
        .unwrap();
    backend.step();
    let events = binder.events.lock().clone();
    assert!(
        events.contains(&(actor, false)),
        "dead host's actor must be unbound: {events:?}"
    );
    assert!(
        route_view.read().unwrap().get(&actor).is_none(),
        "dead host's actor must leave the route view"
    );

    // Host returns: the cached claim re-enters the view, so bind again.
    rt.send_to(directory, membership(peer_id, MemberState::Alive))
        .unwrap();
    backend.step();
    let events = binder.events.lock().clone();
    assert_eq!(
        events
            .iter()
            .filter(|(bound_actor, bound)| *bound_actor == actor && *bound)
            .count(),
        2,
        "returning host's actor must be re-bound: {events:?}"
    );
}
