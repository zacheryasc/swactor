use super::*;
use distribution::directory_actor::DirectoryActor;
use distribution::membership_actor::TransportMembershipActor;
use distribution::registry::RegistryConfig;
use distribution::registry_actor::{RegistryActor, RegistryView};
use distribution::swim::actor::MembershipChanged;
use distribution::transport_bridge::{OutboxPeerDirectory, OutboxRouteBinder, RouteViewTransport};
use distribution::types::MemberState;
use std::sync::{RwLock, mpsc};
use swactor::actor::ActorInterface;
use swactor::runtime::{Ctx, RuntimeConfig, RuntimeParts};
use swactor::std::StdExtension;
use swactor_engine::{Engine, TaskSpawner, TokioBackend, TokioConfig};
use swactor_transport::{CodecRemoteSink, JsonCodec, NetworkMessage, TransportRouter};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Application(u64);

impl NetworkMessage for Application {
    fn type_tag() -> &'static str {
        "iroh_driver::reconnect_tests::Application"
    }
}

struct ApplicationInbox(mpsc::Sender<u64>);

impl ActorInterface for ApplicationInbox {
    type Incoming = Application;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx<'_>, message: Application) {
        self.0.send(message.0).unwrap();
    }
}

struct MembershipProbe {
    actor: TransportMembershipActor,
    joins: Arc<AtomicU64>,
    failures: mpsc::Sender<NodeId>,
}

impl ActorInterface for MembershipProbe {
    type Incoming = SwimIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, message: SwimIn) {
        match &message {
            SwimIn::JoinRequest(_) => {
                self.joins.fetch_add(1, Ordering::SeqCst);
            }
            SwimIn::SendFailed { to } => {
                let _ = self.failures.send(*to);
            }
            _ => {}
        }
        self.actor.handle(ctx, message);
    }
}

struct MembershipFanout {
    directory: ActorAddress,
    registry: ActorAddress,
    events: mpsc::Sender<MembershipChanged>,
}

impl ActorInterface for MembershipFanout {
    type Incoming = MembershipChanged;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, message: MembershipChanged) {
        ctx.send(self.directory, DirectoryIn::Membership(message.clone()))
            .unwrap();
        ctx.send(self.registry, RegistryIn::Membership(message.clone()))
            .unwrap();
        let _ = self.events.send(message);
    }
}

struct EventNode {
    driver: IrohDriver,
    rt: Runtime,
    directory: ActorAddress,
    registry: ActorAddress,
    application: ActorAddress,
    received: mpsc::Receiver<u64>,
    membership: mpsc::Receiver<MembershipChanged>,
    failures: mpsc::Receiver<NodeId>,
    joins: Arc<AtomicU64>,
    routes: RouteView,
    names: RegistryView,
    outbox: Outbox,
    _engine: Engine,
}

impl EventNode {
    fn new(tasks: TaskSpawner, engine: Engine, rt: Runtime) -> Self {
        Self::with_adapter(tasks, engine, rt, true)
    }

    fn with_adapter(tasks: TaskSpawner, engine: Engine, rt: Runtime, start: bool) -> Self {
        let mut driver = IrohDriver::with_engine(
            tasks,
            engine.handle(),
            IrohDriverConfig {
                secret_key: None,
                relay_mode: RelayMode::Disabled,
                bind_port: None,
                node: DistributedNodeConfig::default(),
                peer_auth: None,
                additional_alpns: Vec::new(),
            },
        )
        .unwrap();
        let node = driver.node_id();
        let mut codec = actor_codec_registry();
        codec
            .register::<Application, _>(JsonCodec::default())
            .unwrap();
        let codec = Arc::new(codec);
        let router = Arc::new(TransportRouter::new());
        rt.set_remote_sink(Arc::new(CodecRemoteSink::new(
            codec.clone(),
            router.clone(),
        )));
        let outbox: Outbox = Arc::new(Default::default());
        let routes: RouteView = Arc::new(RwLock::new(HashMap::new()));
        let peers = Arc::new(OutboxPeerDirectory::new(router.clone(), outbox.clone()));
        let binder = Arc::new(OutboxRouteBinder::new(
            router,
            Arc::new(RouteViewTransport::new(routes.clone(), outbox.clone())),
        ));
        // Default dissemination has no registration-triggered broadcasts. No
        // tick task is installed: new maps can cross only via causal SyncTo.
        let directory = rt
            .spawn(DirectoryActor::new(
                node,
                peers.clone(),
                routes.clone(),
                binder,
            ))
            .unwrap();
        let names: RegistryView = Arc::new(RwLock::new(Default::default()));
        let registry = rt
            .spawn(
                RegistryActor::new(node, RegistryConfig::default(), peers.clone())
                    .with_view(names.clone()),
            )
            .unwrap();
        let joins = Arc::new(AtomicU64::new(0));
        let (failures_tx, failures) = mpsc::channel();
        let swim = rt
            .spawn(MembershipProbe {
                actor: TransportMembershipActor::new(node, peers, ActorAddress::default()),
                joins: joins.clone(),
                failures: failures_tx,
            })
            .unwrap();
        let (membership_tx, membership) = mpsc::channel();
        let fanout = rt
            .spawn(MembershipFanout {
                directory,
                registry,
                events: membership_tx,
            })
            .unwrap();
        rt.send_to(swim, SwimIn::Subscribe { observer: fanout })
            .unwrap();
        let (received_tx, received) = mpsc::channel();
        let application = rt.spawn(ApplicationInbox(received_tx)).unwrap();
        let ingress = HashMap::from([
            ("swactor_dist::JoinRequest".to_owned(), swim),
            ("swactor_dist::JoinResponse".to_owned(), swim),
            ("swactor_dist::MembershipGossip".to_owned(), swim),
            ("swactor_dist::DirectoryGossip".to_owned(), directory),
            ("swactor_dist::DirectoryRouteOffer".to_owned(), directory),
            ("swactor_dist::DirectoryRouteAck".to_owned(), directory),
            ("swactor_dist::RegistryGossip".to_owned(), registry),
        ]);
        driver.enable_actor_bridge(ActorBridgeConfig {
            runtime: rt.clone(),
            codec,
            routes: ingress,
            swim,
            relay_mirror: Arc::new(RwLock::new(HashMap::new())),
            route_view: routes.clone(),
            outbox: outbox.clone(),
        });
        if start {
            driver.install_actor_bridge();
        }
        Self {
            driver,
            rt,
            directory,
            registry,
            application,
            received,
            membership,
            failures,
            joins,
            routes,
            names,
            outbox,
            _engine: engine,
        }
    }

    fn expect_membership(&self, peer: NodeId, state: MemberState) {
        let event = self.membership.recv().expect("transport membership event");
        assert_eq!((event.node_id, event.state), (peer, state));
    }

    fn send_application(&self, to: &Self, sequence: u64) {
        self.outbox.lock().unwrap().push(OutFrame {
            to: to.driver.node_id(),
            dest: to.application,
            type_tag: Application::type_tag().to_owned(),
            payload: serde_json::to_vec(&Application(sequence)).unwrap(),
        });
        self.driver.wake.notify_one();
    }

    fn register(&self, actor: ActorAddress, name: &str) {
        self.rt
            .send_to(
                self.directory,
                DirectoryIn::Register(self.driver.register_actor(actor, 1)),
            )
            .unwrap();
        self.rt
            .send_to(
                self.registry,
                RegistryIn::RegisterName {
                    name: name.to_owned(),
                    actor_addr: actor,
                },
            )
            .unwrap();
        self.expect_maps(actor, self.driver.node_id(), name);
    }

    fn expect_maps(&self, actor: ActorAddress, host: NodeId, name: &str) {
        let (changed, changes) = mpsc::channel();
        let changed: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _ = changed.send(());
        });
        self.rt
            .send_to(
                self.directory,
                DirectoryIn::WatchRoutes {
                    changed: Arc::downgrade(&changed),
                },
            )
            .unwrap();
        self.rt
            .send_to(
                self.registry,
                RegistryIn::WatchName {
                    name: name.to_owned(),
                    changed: Arc::downgrade(&changed),
                },
            )
            .unwrap();
        // Subscribe before checking; both actors also notify on registration.
        // Each recheck is caused by a concrete publication, never a clock.
        while !self.has_maps(actor, host, name) {
            changes.recv().expect("directory or registry publication");
        }
    }

    fn has_maps(&self, actor: ActorAddress, host: NodeId, name: &str) -> bool {
        self.routes.read().unwrap().get(&actor) == Some(&host)
            && self
                .names
                .read()
                .unwrap()
                .entries
                .iter()
                .any(|entry| entry.name == name && entry.actor_addr == actor && !entry.tombstone)
    }

    fn connection(&self, peer: NodeId) -> CachedConnection {
        self.driver.conns.lock().connections[&peer].clone()
    }
}

impl Drop for EventNode {
    fn drop(&mut self) {
        self.driver.shutdown();
    }
}

fn node_engine() -> (Engine, Runtime) {
    let parts =
        RuntimeParts::new(RuntimeConfig::default()).with_extension(Arc::new(StdExtension::new()));
    let rt = parts.runtime().clone();
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    (engine, rt)
}

#[test]
fn known_cluster_app_reconnect_rehandshakes_and_syncs_without_ticks() {
    let (engine, rt) = node_engine();
    let a = EventNode::new(engine.task_spawner(), engine, rt);
    let (engine, rt) = node_engine();
    let b = EventNode::new(engine.task_spawner(), engine, rt);
    let a_id = a.driver.node_id();
    let b_id = b.driver.node_id();
    a.driver.join(&[b.driver.endpoint_addr()]);
    a.expect_membership(b_id, MemberState::Alive);
    b.expect_membership(a_id, MemberState::Alive);

    // Exercise both actor-safe explicit dialing and application-triggered
    // redial. Neither phase sends Join, SWIM probes, or protocol ticks.
    for (round, explicit_connect) in [(1, true), (2, false)] {
        let old = a.connection(b_id);
        old.conn
            .close(iroh::endpoint::VarInt::from_u32(0), b"test reconnect");
        a.expect_membership(b_id, MemberState::Dead);
        b.expect_membership(a_id, MemberState::Dead);
        assert!(!a.driver.conns.lock().connections.contains_key(&b_id));
        assert!(!b.driver.conns.lock().connections.contains_key(&a_id));
        let a_joins = a.joins.load(Ordering::SeqCst);
        let b_joins = b.joins.load(Ordering::SeqCst);
        let a_actor = ActorAddress([20 + round; 32]);
        let b_actor = ActorAddress([30 + round; 32]);
        let a_name = format!("a-{round}");
        let b_name = format!("b-{round}");
        a.register(a_actor, &a_name);
        b.register(b_actor, &b_name);
        if explicit_connect {
            a.driver.connect_peer(b.driver.endpoint_addr());
        }
        a.send_application(&b, u64::from(round));
        assert_eq!(b.received.recv().unwrap(), u64::from(round));
        a.expect_membership(b_id, MemberState::Alive);
        b.expect_membership(a_id, MemberState::Alive);
        a.expect_maps(b_actor, b_id, &b_name);
        b.expect_maps(a_actor, a_id, &a_name);
        assert_eq!(a.joins.load(Ordering::SeqCst), a_joins + 1);
        assert_eq!(b.joins.load(Ordering::SeqCst), b_joins + 1);

        // An old reader's late close cannot evict or disconnect the replacement.
        let current = a.connection(b_id);
        assert_ne!(old.generation, current.generation);
        a.driver.evict.lock().push(FailedConnection {
            node_id: b_id,
            generation: old.generation,
        });
        a.driver.wake.notify_one();
        a.driver.connect_peer(b.driver.endpoint_addr());
        a.send_application(&b, 100 + u64::from(round));
        assert_eq!(b.received.recv().unwrap(), 100 + u64::from(round));
        assert_eq!(a.connection(b_id).generation, current.generation);
        assert!(a.membership.try_recv().is_err());
    }
}

#[test]
fn actor_only_connections_stay_out_of_membership_and_release_failed_admission() {
    let (engine, rt) = node_engine();
    let child = EventNode::new(engine.task_spawner(), engine, rt);
    let (engine, rt) = node_engine();
    let host = EventNode::new(engine.task_spawner(), engine, rt);
    let child_id = child.driver.node_id();
    let host_id = host.driver.node_id();
    child.driver.connect_peer(host.driver.endpoint_addr());
    child.send_application(&host, 1);
    assert_eq!(host.received.recv().unwrap(), 1);
    host.send_application(&child, 2);
    assert_eq!(child.received.recv().unwrap(), 2);
    child
        .connection(host_id)
        .conn
        .close(iroh::endpoint::VarInt::from_u32(0), b"actor-only close");
    assert_eq!(child.failures.recv().unwrap(), host_id);
    assert_eq!(host.failures.recv().unwrap(), child_id);
    for node in [&child, &host] {
        assert!(node.membership.try_recv().is_err());
        assert_eq!(node.joins.load(Ordering::SeqCst), 0);
        assert!(node.driver.conns.lock().known_cluster_peers.is_empty());
    }
}

#[test]
fn exhausted_dial_discards_stale_frames_but_preserves_fresh_requests() {
    // Pause only the adapter, not the real actors or QUIC endpoints, to place
    // exhaustion exactly between retained traffic and a fresh actor request.
    let (engine, rt) = node_engine();
    let source = EventNode::with_adapter(engine.task_spawner(), engine, rt, false);
    let (engine, rt) = node_engine();
    let recovered = EventNode::new(engine.task_spawner(), engine, rt);
    let (engine, rt) = node_engine();
    let unaffected = EventNode::new(engine.task_spawner(), engine, rt);
    let failed_peer = recovered.driver.node_id();
    let _ = source
        .driver
        .cache_peer_address(&recovered.driver.endpoint_addr());
    let _ = source
        .driver
        .cache_peer_address(&unaffected.driver.endpoint_addr());
    let frame = |node: &EventNode, sequence| OutFrame {
        to: node.driver.node_id(),
        dest: node.application,
        type_tag: Application::type_tag().to_owned(),
        payload: serde_json::to_vec(&Application(sequence)).unwrap(),
    };
    let bridge = source.driver.actor_bridge.as_ref().unwrap();
    bridge
        .pending_outbound
        .lock()
        .extend([frame(&recovered, 1), frame(&unaffected, 2)]);
    source.outbox.lock().unwrap().push(frame(&recovered, 3));
    // All three bounded dial paths report their terminal outcome here.
    bridge.dial_failed(failed_peer);
    assert_eq!(source.failures.recv().unwrap(), failed_peer);
    source.driver.install_actor_bridge();
    assert_eq!(unaffected.received.recv().unwrap(), 2);
    // Without discarding the stale frame, ordered delivery would yield 1
    // before 3. The new request is the only reason to retry this failed peer.
    assert_eq!(recovered.received.recv().unwrap(), 3);
    assert!(source.failures.try_recv().is_err());
}

#[test]
fn route_readiness_frames_require_the_authenticated_carrier() {
    let owner = Keypair::from_bytes(&[1; 32]);
    let other = Keypair::from_bytes(&[2; 32]).node_id();
    let claim = owner.sign_directory_entry(ActorAddress([7; 32]), 9);
    let offer: Box<dyn Any + Send> = Box::new(DirectoryIn::RouteOffer(DirectoryRouteOffer {
        claim: claim.clone(),
        attempt: 11,
    }));
    let ack: Box<dyn Any + Send> = Box::new(DirectoryIn::RouteAck(DirectoryRouteAck {
        from: owner.node_id(),
        actor: claim.actor_addr,
        generation: claim.generation,
        attempt: 11,
    }));
    for message in [offer, ack] {
        assert!(directory_sender_is_verified(&message, owner.node_id()));
        assert!(!directory_sender_is_verified(&message, other));
    }
}
