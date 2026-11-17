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

#[test]
fn verified_generation_changes_wake_observers_without_route_or_clock_changes() {
    let parts =
        RuntimeParts::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()));
    let rt = parts.runtime().clone();
    let backend = SteppingBackend::new();
    let _engine = Engine::new(parts, backend.clone()).unwrap();
    let key = Keypair::from_bytes(&[3; 32]);
    let actor = ActorAddress([9; 32]);
    let directory = DirectoryActor::new(
        key.node_id(),
        Arc::new(SharedPeerDirectory::new()),
        Arc::new(std::sync::RwLock::new(HashMap::new())),
        Arc::new(RecordingBinder {
            events: Mutex::new(Vec::new()),
        }),
    );
    let claims = directory.claims();
    let directory = rt.spawn(directory).unwrap();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let observed = observations.clone();
    let changed: Arc<dyn Fn() + Send + Sync> =
        Arc::new(move || observed.lock().push(claims.location(&actor)));
    rt.send_to(
        directory,
        DirectoryIn::WatchRoutes {
            changed: Arc::downgrade(&changed),
        },
    )
    .unwrap();
    backend.step();
    rt.send_to(
        directory,
        DirectoryIn::Register(key.sign_directory_entry(actor, 1)),
    )
    .unwrap();
    backend.step();
    let mut forged = key.sign_directory_entry(actor, 1);
    forged.generation = 99;
    rt.send_to(directory, DirectoryIn::Register(forged))
        .unwrap();
    rt.send_to(
        directory,
        DirectoryIn::Register(key.sign_directory_entry(actor, 2)),
    )
    .unwrap();
    backend.step();
    rt.send_to(
        directory,
        DirectoryIn::Register(key.sign_directory_entry(actor, 1)),
    )
    .unwrap();
    backend.step();
    assert_eq!(
        *observations.lock(),
        vec![None, Some((key.node_id(), 1)), Some((key.node_id(), 2))],
        "only verified winning generations may wake the authority observer"
    );
    drop(changed);
    rt.send_to(
        directory,
        DirectoryIn::Register(key.sign_directory_entry(actor, 3)),
    )
    .unwrap();
    backend.step();
    assert_eq!(
        *observations.lock(),
        vec![None, Some((key.node_id(), 1)), Some((key.node_id(), 2))],
        "dropping the observation must end callbacks"
    );
}

mod reply_route_handshake {
    use super::{RecordingBinder, membership};
    use std::any::TypeId;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use distribution::crypto::{Keypair, KeypairExt, verify_directory_entry};
    use distribution::directory_actor::{DirectoryActor, DirectoryIn};
    use distribution::messages::{
        DirectoryRouteAck, DirectoryRouteOffer, actor_codec_registry, distribution_codec_registry,
    };
    use distribution::swim::actor::{PeerDirectory, SharedPeerDirectory};
    use distribution::transport_bridge::{
        OutFrame, Outbox, OutboxPeerDirectory, RouteView, peer_addr,
    };
    use distribution::types::{DirectoryEntry, MemberState, NodeId};
    use parking_lot::Mutex;
    use swactor::actor::ActorAddress;
    use swactor::runtime::{Runtime, RuntimeConfig, RuntimeParts};
    use swactor::std::StdExtension;
    use swactor_engine::{Engine, SteppingBackend};
    use swactor_transport::{CodecRemoteSink, NetworkMessage, TransportRouter};

    struct DirectoryNode {
        rt: Runtime,
        router: Arc<TransportRouter>,
        directory: ActorAddress,
        backend: SteppingBackend,
        _engine: Engine,
        outbox: Outbox,
        view: RouteView,
        binder: Arc<RecordingBinder>,
    }

    impl DirectoryNode {
        fn new(id: NodeId) -> Self {
            Self::with_peer_directory(id, None)
        }

        fn with_peer_directory(id: NodeId, peers: Option<Arc<dyn PeerDirectory>>) -> Self {
            let parts = RuntimeParts::new(RuntimeConfig::default())
                .with_extension(Arc::new(StdExtension::new()));
            let rt = parts.runtime().clone();
            let router = Arc::new(TransportRouter::new());
            rt.set_remote_sink(Arc::new(CodecRemoteSink::new(
                Arc::new(actor_codec_registry()),
                router.clone(),
            )));
            let backend = SteppingBackend::new();
            let engine = Engine::new(parts, backend.clone()).unwrap();
            let outbox = Outbox::default();
            let view: RouteView = Arc::new(std::sync::RwLock::new(HashMap::new()));
            let binder = Arc::new(RecordingBinder {
                events: Mutex::new(Vec::new()),
            });
            let directory = rt
                .spawn(
                    DirectoryActor::new(
                        id,
                        peers.unwrap_or_else(|| {
                            Arc::new(OutboxPeerDirectory::new(router.clone(), outbox.clone()))
                        }),
                        view.clone(),
                        binder.clone(),
                    )
                    .with_event_dissemination(),
                )
                .unwrap();
            // Observe synchronous egress, not a later mailbox delivery: an ACK
            // queued before ensure_routable returns must fail this assertion.
            let observed_outbox = Arc::downgrade(&outbox);
            let observed_view = view.clone();
            let observed_binder = binder.clone();
            outbox.set_wake(Arc::new(move || {
                let outbox = observed_outbox.upgrade().unwrap();
                let frames = outbox.lock().unwrap();
                let Some(frame) = frames.last() else {
                    return;
                };
                if frame.type_tag == DirectoryRouteAck::type_tag() {
                    let ack: DirectoryRouteAck = serde_json::from_slice(&frame.payload).unwrap();
                    assert_eq!(ack.from, id);
                    assert_eq!(
                        observed_view.read().unwrap().get(&ack.actor),
                        Some(&frame.to)
                    );
                    assert_eq!(
                        observed_binder
                            .events
                            .lock()
                            .iter()
                            .rev()
                            .find(|(actor, _)| *actor == ack.actor),
                        Some(&(ack.actor, true)),
                        "ACK must be enqueued only after its route binder is installed"
                    );
                }
            }));
            Self {
                rt,
                router,
                directory,
                backend,
                _engine: engine,
                outbox,
                view,
                binder,
            }
        }

        fn send(&self, msg: DirectoryIn) {
            self.rt.send_to(self.directory, msg).unwrap();
            self.backend.step();
        }

        fn offer(&self, offer: DirectoryRouteOffer) {
            let (tag, bytes) = distribution_codec_registry()
                .encode(TypeId::of::<DirectoryRouteOffer>(), Box::new(offer))
                .unwrap();
            assert_eq!(tag, DirectoryRouteOffer::type_tag());
            let incoming = actor_codec_registry().decode(&tag, &bytes).unwrap();
            self.rt.deliver_raw(self.directory, incoming).unwrap();
            self.backend.step();
        }

        fn ack(&self, ack: DirectoryRouteAck) {
            let (tag, bytes) = distribution_codec_registry()
                .encode(TypeId::of::<DirectoryRouteAck>(), Box::new(ack))
                .unwrap();
            assert_eq!(tag, DirectoryRouteAck::type_tag());
            let incoming = actor_codec_registry().decode(&tag, &bytes).unwrap();
            self.rt.deliver_raw(self.directory, incoming).unwrap();
            self.backend.step();
        }

        fn drain(&self, tag: &str) -> Vec<OutFrame> {
            self.outbox
                .lock()
                .unwrap()
                .drain(..)
                .filter(|frame| frame.type_tag == tag)
                .collect()
        }

        fn expect_ack(&self, to: NodeId, actor: ActorAddress, generation: u64, attempt: u64) {
            let frames = self.drain(DirectoryRouteAck::type_tag());
            assert_eq!(frames.len(), 1);
            assert_eq!(frames[0].to, to);
            let decoded = distribution_codec_registry()
                .decode(&frames[0].type_tag, &frames[0].payload)
                .unwrap();
            let ack = decoded.downcast_ref::<DirectoryRouteAck>().unwrap();
            assert_eq!(
                (ack.actor, ack.generation, ack.attempt),
                (actor, generation, attempt)
            );
        }

        fn ensure(
            &self,
            claim: DirectoryEntry,
            peer: NodeId,
            attempt: u64,
            completed: &Arc<dyn Fn(Result<(), ()>) + Send + Sync>,
        ) {
            self.send(DirectoryIn::EnsureRoute {
                claim,
                peer,
                attempt,
                completed: Arc::downgrade(completed),
            });
        }
    }

    #[test]
    fn duplicate_offer_waits_for_membership_and_binding_then_acknowledges_again() {
        let owner = Keypair::from_bytes(&[31; 32]);
        let receiver = DirectoryNode::new(NodeId([32; 32]));
        let actor = ActorAddress([33; 32]);
        let claim = owner.sign_directory_entry(actor, 7);
        // The offer does not change the already-cached claim. It still needs a
        // readiness event once Alive makes that claim routable.
        receiver.send(DirectoryIn::Register(claim.clone()));
        let offer = DirectoryRouteOffer { claim, attempt: 4 };
        receiver.offer(offer.clone());
        receiver.offer(offer.clone());
        assert!(receiver.drain(DirectoryRouteAck::type_tag()).is_empty());
        assert!(!receiver.view.read().unwrap().contains_key(&actor));
        assert!(receiver.binder.events.lock().is_empty());

        receiver.send(membership(owner.node_id(), MemberState::Alive));
        receiver.expect_ack(owner.node_id(), actor, 7, 4);
        receiver.offer(offer);
        receiver.expect_ack(owner.node_id(), actor, 7, 4);
        assert_eq!(*receiver.binder.events.lock(), vec![(actor, true)]);
    }

    #[test]
    fn forged_losing_and_departed_offers_cannot_acknowledge() {
        let owner = Keypair::from_bytes(&[41; 32]);
        let receiver = DirectoryNode::new(NodeId([42; 32]));
        let actor = ActorAddress([43; 32]);
        let mut forged = owner.sign_directory_entry(actor, 1);
        forged.generation = 99;
        receiver.offer(DirectoryRouteOffer {
            claim: forged,
            attempt: 1,
        });
        receiver.send(membership(owner.node_id(), MemberState::Alive));
        assert!(!receiver.view.read().unwrap().contains_key(&actor));
        assert!(receiver.drain(DirectoryRouteAck::type_tag()).is_empty());

        receiver.send(membership(owner.node_id(), MemberState::Dead));
        receiver.offer(DirectoryRouteOffer {
            claim: owner.sign_directory_entry(actor, 1),
            attempt: 2,
        });
        receiver.send(DirectoryIn::Register(owner.sign_directory_entry(actor, 2)));
        receiver.send(membership(owner.node_id(), MemberState::Alive));
        assert!(receiver.drain(DirectoryRouteAck::type_tag()).is_empty());
        receiver.offer(DirectoryRouteOffer {
            claim: owner.sign_directory_entry(actor, 1),
            attempt: 3,
        });
        assert!(receiver.drain(DirectoryRouteAck::type_tag()).is_empty());

        receiver.send(membership(owner.node_id(), MemberState::Dead));
        receiver.offer(DirectoryRouteOffer {
            claim: owner.sign_directory_entry(actor, 2),
            attempt: 4,
        });
        receiver.send(membership(owner.node_id(), MemberState::Dead));
        receiver.send(membership(owner.node_id(), MemberState::Alive));
        assert!(receiver.drain(DirectoryRouteAck::type_tag()).is_empty());
        receiver.offer(DirectoryRouteOffer {
            claim: owner.sign_directory_entry(actor, 2),
            attempt: 5,
        });
        receiver.expect_ack(owner.node_id(), actor, 2, 5);
    }

    #[test]
    fn ensure_route_is_one_shot_and_rejects_stale_or_mismatched_acknowledgments() {
        let owner = Keypair::from_bytes(&[51; 32]);
        let sender = DirectoryNode::new(owner.node_id());
        let peer = NodeId([52; 32]);
        let next_peer = NodeId([53; 32]);
        let actor = ActorAddress([54; 32]);
        let claim = owner.sign_directory_entry(actor, 7);
        let completions = Arc::new(AtomicUsize::new(0));
        let observed = completions.clone();
        let completed: Arc<dyn Fn(Result<(), ()>) + Send + Sync> = Arc::new(move |result| {
            assert_eq!(result, Ok(()));
            observed.fetch_add(1, Ordering::SeqCst);
        });
        sender.ensure(claim.clone(), peer, 10, &completed);
        sender.ensure(claim.clone(), peer, 10, &completed);
        let frames = sender.drain(DirectoryRouteOffer::type_tag());
        assert_eq!(
            frames.len(),
            1,
            "one offer per supplied attempt, without retries"
        );
        assert_eq!(frames[0].to, peer);
        let decoded = distribution_codec_registry()
            .decode(&frames[0].type_tag, &frames[0].payload)
            .unwrap();
        let offer = decoded.downcast_ref::<DirectoryRouteOffer>().unwrap();
        assert!(verify_directory_entry(&offer.claim));
        assert_eq!(
            (
                offer.claim.actor_addr,
                offer.claim.generation,
                offer.attempt
            ),
            (actor, 7, 10)
        );
        assert_eq!(completions.load(Ordering::SeqCst), 0);

        sender.ensure(claim.clone(), peer, 11, &completed);
        let current = DirectoryRouteAck {
            from: peer,
            actor,
            generation: 7,
            attempt: 11,
        };
        for wrong in [
            DirectoryRouteAck {
                attempt: 10,
                ..current.clone()
            },
            DirectoryRouteAck {
                from: next_peer,
                ..current.clone()
            },
            DirectoryRouteAck {
                actor: ActorAddress([55; 32]),
                ..current.clone()
            },
            DirectoryRouteAck {
                generation: 6,
                ..current.clone()
            },
        ] {
            sender.ack(wrong);
        }
        assert_eq!(completions.load(Ordering::SeqCst), 0);
        sender.ack(current.clone());
        sender.ack(current.clone());
        assert_eq!(completions.load(Ordering::SeqCst), 1);

        sender.ensure(claim.clone(), peer, 12, &completed);
        sender.ensure(claim.clone(), next_peer, 13, &completed);
        sender.ack(DirectoryRouteAck {
            attempt: 12,
            ..current.clone()
        });
        sender.ack(current.clone());
        assert_eq!(completions.load(Ordering::SeqCst), 1);
        let next = DirectoryRouteAck {
            from: next_peer,
            attempt: 13,
            ..current
        };
        sender.ack(next.clone());
        assert_eq!(completions.load(Ordering::SeqCst), 2);

        sender.ensure(claim.clone(), next_peer, 14, &completed);
        sender.send(membership(next_peer, MemberState::Dead));
        sender.ack(DirectoryRouteAck {
            attempt: 14,
            ..next.clone()
        });
        assert_eq!(completions.load(Ordering::SeqCst), 2);
        sender.ensure(claim.clone(), next_peer, 15, &completed);
        drop(completed);
        sender.ack(DirectoryRouteAck {
            attempt: 15,
            ..next
        });
        assert_eq!(completions.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn ensure_route_rejects_nonlocal_or_forged_claims_and_cannot_cross_wire() {
        let owner = Keypair::from_bytes(&[61; 32]);
        let other = Keypair::from_bytes(&[62; 32]);
        let sender = DirectoryNode::new(owner.node_id());
        let actor = ActorAddress([63; 32]);
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let observed = outcomes.clone();
        let completed: Arc<dyn Fn(Result<(), ()>) + Send + Sync> =
            Arc::new(move |result| observed.lock().push(result));
        let mut forged = owner.sign_directory_entry(actor, 1);
        forged.generation = 2;
        sender.ensure(forged.clone(), other.node_id(), 1, &completed);
        sender.ensure(forged, other.node_id(), 1, &completed);
        sender.ensure(
            other.sign_directory_entry(actor, 1),
            other.node_id(),
            2,
            &completed,
        );
        assert_eq!(*outcomes.lock(), vec![Err(()), Err(())]);
        assert!(sender.drain(DirectoryRouteOffer::type_tag()).is_empty());
        assert!(
            actor_codec_registry()
                .encode(
                    TypeId::of::<DirectoryIn>(),
                    Box::new(DirectoryIn::EnsureRoute {
                        claim: owner.sign_directory_entry(actor, 1),
                        peer: other.node_id(),
                        attempt: 3,
                        completed: Arc::downgrade(&completed),
                    }),
                )
                .is_err()
        );
    }

    #[test]
    fn ensure_route_reports_missing_peer_and_rejected_send_once_without_retrying() {
        let owner = Keypair::from_bytes(&[71; 32]);
        let peer = NodeId([72; 32]);
        let actor = ActorAddress([73; 32]);
        let peers = Arc::new(SharedPeerDirectory::new());
        let sender = DirectoryNode::with_peer_directory(owner.node_id(), Some(peers.clone()));
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let observed = outcomes.clone();
        let completed: Arc<dyn Fn(Result<(), ()>) + Send + Sync> =
            Arc::new(move |result| observed.lock().push(result));
        let claim = owner.sign_directory_entry(actor, 1);

        sender.ensure(claim.clone(), peer, 1, &completed);
        sender.ensure(claim.clone(), peer, 1, &completed);
        assert_eq!(
            *outcomes.lock(),
            vec![Err(())],
            "missing peer must fail once"
        );

        // Peer lookup now succeeds, but CodecRemoteSink has no transport route.
        peers.bind(peer, peer_addr(peer), 1);
        sender.ensure(claim.clone(), peer, 2, &completed);
        sender.ensure(claim.clone(), peer, 2, &completed);
        sender.ack(DirectoryRouteAck {
            from: peer,
            actor,
            generation: 1,
            attempt: 2,
        });
        assert_eq!(*outcomes.lock(), vec![Err(()), Err(())]);
        assert!(sender.drain(DirectoryRouteOffer::type_tag()).is_empty());

        // Making egress usable is not permission to retry a failed attempt.
        OutboxPeerDirectory::new(sender.router.clone(), sender.outbox.clone()).resolve(&peer);
        sender.send(membership(peer, MemberState::Alive));
        assert!(sender.drain(DirectoryRouteOffer::type_tag()).is_empty());
        sender.ensure(claim, peer, 3, &completed);
        assert_eq!(sender.drain(DirectoryRouteOffer::type_tag()).len(), 1);
        assert_eq!(*outcomes.lock(), vec![Err(()), Err(())]);
        sender.ack(DirectoryRouteAck {
            from: peer,
            actor,
            generation: 1,
            attempt: 3,
        });
        assert_eq!(*outcomes.lock(), vec![Err(()), Err(()), Ok(())]);
    }

    #[test]
    fn rejected_ack_send_remains_pending_until_external_publication() {
        let owner = Keypair::from_bytes(&[81; 32]);
        let actor = ActorAddress([82; 32]);
        let peers = Arc::new(SharedPeerDirectory::new());
        peers.bind(owner.node_id(), peer_addr(owner.node_id()), 1);
        let receiver = DirectoryNode::with_peer_directory(NodeId([83; 32]), Some(peers));
        receiver.send(membership(owner.node_id(), MemberState::Alive));
        receiver.offer(DirectoryRouteOffer {
            claim: owner.sign_directory_entry(actor, 1),
            attempt: 4,
        });
        assert_eq!(
            receiver.view.read().unwrap().get(&actor),
            Some(&owner.node_id())
        );
        assert!(receiver.drain(DirectoryRouteAck::type_tag()).is_empty());

        OutboxPeerDirectory::new(receiver.router.clone(), receiver.outbox.clone())
            .resolve(&owner.node_id());
        receiver.backend.step();
        assert!(
            receiver.drain(DirectoryRouteAck::type_tag()).is_empty(),
            "send failure must not schedule its own retry"
        );
        receiver.send(membership(owner.node_id(), MemberState::Alive));
        receiver.expect_ack(owner.node_id(), actor, 1, 4);
        receiver.send(membership(owner.node_id(), MemberState::Alive));
        assert!(receiver.drain(DirectoryRouteAck::type_tag()).is_empty());
    }
}
