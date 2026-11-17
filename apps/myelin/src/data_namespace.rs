use parking_lot::Mutex;
use std::path::Path;
use std::sync::{Arc, OnceLock};

use crate::orchestration::distribution_stack::DistributionRuntimeStack;
use data_plane::blob_transfer::BlobTransferSender;
use data_plane::control::{DataNamespaceService, DataPlaneControl};
use data_plane::namespace::{
    NamespaceClient, NamespaceClientActor, NamespaceClientIn, NamespaceDiscovery,
    NamespaceRouteReadiness,
};
use data_plane::source::BlobSourcePublisher;
use distribution::directory_actor::{DirectoryClaims, DirectoryIn};
use distribution::registry_actor::{RegistryIn, RegistryView};
use distribution::transport_bridge::RouteView;
use distribution::types::{DirectoryEntry, NodeId};
use iroh_driver::{ActorRegistrar, ConnectionWatch, IrohBlobTransferSender, IrohDriver};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Runtime};

// TODO: Service? What?
pub(crate) const DATA_DIRECTORY_SERVICE: &str = "swactor.data-directory";
const RECOVERED_SERVICE_TIMESTAMP_BASE: u64 = 1_u64 << 63;

#[derive(Clone)]
struct LocalActorPublisher {
    runtime: Runtime,
    distribution_directory: ActorAddress,
    registrar: ActorRegistrar,
}

impl LocalActorPublisher {
    fn publish(&self, actor: ActorAddress, generation: u64) -> Result<[u8; 32], String> {
        let claim = self.registrar.register_actor(actor, generation);
        self.runtime
            .send_to(self.distribution_directory, DirectoryIn::Register(claim))
            .map_err(|error| error.to_string())?;
        Ok(self.registrar.node_id_bytes())
    }
}

impl BlobSourcePublisher for LocalActorPublisher {
    fn publish_source(&self, source: ActorAddress) -> Result<[u8; 32], String> {
        self.publish(source, 1)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AuthorityRoute {
    directory: ActorAddress,
    epoch: u64,
    host: NodeId,
    connection_generation: u64,
}

struct PendingReplyRoute {
    authority: AuthorityRoute,
    attempt: u64,
    readiness: NamespaceRouteReadiness,
    _callback: Arc<dyn Fn(Result<(), ()>) + Send + Sync>,
}

#[derive(Default)]
struct ReplyRouteState {
    last_attempt: u64,
    pending: Option<PendingReplyRoute>,
}

// TODO: This looks horrible hack job
// TODO: Random oncelocks, an arc mutex? Terrible.
struct NamespaceReplyRoute {
    local_node: NodeId,
    sender: ExternalSender,
    directory: ActorAddress,
    claim: OnceLock<DirectoryEntry>,
    wake: OnceLock<Arc<dyn Fn() + Send + Sync>>,
    state: Arc<Mutex<ReplyRouteState>>,
}

impl NamespaceReplyRoute {
    fn new(runtime: &Runtime, directory: ActorAddress, local_node: NodeId) -> Self {
        Self {
            local_node,
            sender: runtime.create_sender(),
            directory,
            claim: OnceLock::new(),
            wake: OnceLock::new(),
            state: Arc::new(Mutex::new(ReplyRouteState::default())),
        }
    }

    fn reset(&self) {
        self.state.lock().pending = None;
    }

    /// Genuine discovery/transport changes can retry a failed offer. Completion
    /// notifications use the plain wake instead, so failure never self-retries.
    fn observer(&self, wake: Arc<dyn Fn() + Send + Sync>) -> Arc<dyn Fn() + Send + Sync> {
        let state = Arc::downgrade(&self.state);
        Arc::new(move || {
            if let Some(state) = state.upgrade() {
                let mut state = state.lock();
                if state.pending.as_ref().is_some_and(|pending| {
                    pending.readiness == NamespaceRouteReadiness::Unavailable
                }) {
                    state.pending = None;
                }
            }
            wake();
        })
    }

    fn prepare(&self, authority: AuthorityRoute) -> NamespaceRouteReadiness {
        if authority.host == self.local_node {
            self.reset();
            return NamespaceRouteReadiness::Ready;
        }
        let (Some(claim), Some(wake)) = (self.claim.get(), self.wake.get()) else {
            return NamespaceRouteReadiness::Pending;
        };
        let mut state = self.state.lock();
        if let Some(pending) = &state.pending
            && pending.authority == authority
        {
            return pending.readiness;
        }
        let Some(attempt) = state.last_attempt.checked_add(1) else {
            state.pending = None;
            return NamespaceRouteReadiness::Unavailable;
        };
        state.last_attempt = attempt;
        let weak_state = Arc::downgrade(&self.state);
        let wake = Arc::clone(wake);
        let completed: Arc<dyn Fn(Result<(), ()>) + Send + Sync> = Arc::new(move |result| {
            let Some(state) = weak_state.upgrade() else {
                return;
            };
            {
                let mut state = state.lock();
                let Some(pending) = state.pending.as_mut() else {
                    return;
                };
                if pending.authority != authority
                    || pending.attempt != attempt
                    || pending.readiness != NamespaceRouteReadiness::Pending
                {
                    return;
                }
                pending.readiness = if result.is_ok() {
                    NamespaceRouteReadiness::Ready
                } else {
                    NamespaceRouteReadiness::Unavailable
                };
            }
            wake();
        });
        state.pending = Some(PendingReplyRoute {
            authority,
            attempt,
            readiness: NamespaceRouteReadiness::Pending,
            _callback: completed.clone(),
        });
        drop(state);
        if self
            .sender
            .send_to(
                self.directory,
                DirectoryIn::EnsureRoute {
                    claim: claim.clone(),
                    peer: authority.host,
                    attempt,
                    completed: Arc::downgrade(&completed),
                },
            )
            .is_err()
        {
            self.reset();
            return NamespaceRouteReadiness::Unavailable;
        }
        NamespaceRouteReadiness::Pending
    }
}

struct RegistryNamespaceDiscovery {
    view: RegistryView,
    claims: DirectoryClaims,
    routes: RouteView,
    connection_watch: OnceLock<ConnectionWatch>,
    connection_generation: Arc<dyn Fn(NodeId) -> Option<u64> + Send + Sync>,
    reply_route: NamespaceReplyRoute,
}

impl RegistryNamespaceDiscovery {
    fn current_authority(&self) -> Option<AuthorityRoute> {
        let view = self.view.read().expect("registry view poisoned");
        let binding = view
            .entries
            .iter()
            .find(|entry| entry.name == DATA_DIRECTORY_SERVICE && !entry.tombstone)?;
        let (host, generation) = self.claims.location(&binding.actor_addr)?;
        let connection_generation = (self.connection_generation)(host)?;
        if host != binding.node_id
            || self
                .routes
                .read()
                .expect("route view poisoned")
                .get(&binding.actor_addr)
                != Some(&host)
        {
            return None;
        }
        // Registry timestamps are Lamport ordering, not durable epochs: a
        // repeated publication or tombstone takeover advances them. The
        // authority signs its unchanged epoch into the actor-location claim.
        let epoch = generation
            .checked_sub(RECOVERED_SERVICE_TIMESTAMP_BASE)
            .filter(|epoch| *epoch != 0)?;
        Some(AuthorityRoute {
            directory: binding.actor_addr,
            epoch,
            host,
            connection_generation,
        })
    }
}

impl NamespaceDiscovery for RegistryNamespaceDiscovery {
    fn current_directory(&self) -> Option<ActorAddress> {
        let authority = self.current_authority();
        if authority.is_none() {
            self.reply_route.reset();
        }
        authority.map(|authority| authority.directory)
    }

    fn prepare_reply_route(&self, directory: ActorAddress) -> NamespaceRouteReadiness {
        match self.current_authority() {
            Some(authority) if authority.directory == directory => {
                self.reply_route.prepare(authority)
            }
            _ => {
                self.reply_route.reset();
                NamespaceRouteReadiness::Unavailable
            }
        }
    }

    fn accepts_authority_epoch(&self, epoch: u64) -> bool {
        self.current_authority()
            .is_some_and(|authority| epoch == authority.epoch)
    }
}

struct NamespaceServicePublisher {
    registry: ActorAddress,
    view: RegistryView,
    directory: ActorAddress,
    timestamp: u64,
    /// Persist until each peer acknowledges the exact current binding.
    rebind: Vec<swactor_transport::NodeId>,
}

impl ActorInterface for NamespaceServicePublisher {
    type Incoming = RegistryIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, message: RegistryIn) {
        match message {
            RegistryIn::NameAcknowledged { entry, peer } => {
                let current = entry.name == DATA_DIRECTORY_SERVICE
                    && entry.actor_addr == self.directory
                    && !entry.tombstone
                    && self
                        .view
                        .read()
                        .expect("registry view poisoned")
                        .entries
                        .iter()
                        .any(|current| {
                            current.name == entry.name
                                && current.actor_addr == entry.actor_addr
                                && current.node_id == entry.node_id
                                && current.timestamp == entry.timestamp
                                && current.generation == entry.generation
                                && !current.tombstone
                        });
                if current {
                    self.rebind.retain(|pending| *pending != peer);
                }
            }
            RegistryIn::RegisterNameAt {
                name,
                actor_addr,
                timestamp,
            } if name == DATA_DIRECTORY_SERVICE
                && actor_addr == self.directory
                && timestamp == self.timestamp =>
            {
                let published = self
                    .view
                    .read()
                    .expect("registry view poisoned")
                    .entries
                    .iter()
                    .any(|entry| {
                        entry.name == DATA_DIRECTORY_SERVICE
                            && !entry.tombstone
                            && entry.actor_addr == self.directory
                    });
                if !published {
                    let _ = ctx.send(
                        self.registry,
                        RegistryIn::RegisterNameAt {
                            name: DATA_DIRECTORY_SERVICE.to_owned(),
                            actor_addr: self.directory,
                            timestamp: self.timestamp,
                        },
                    );
                }
                // Registry and transport observations retry outstanding recovery
                // obligations; elapsed time never abandons a persisted worker.
                if !self.rebind.is_empty() {
                    let _ = ctx.send(
                        self.registry,
                        RegistryIn::DisseminateNameTo {
                            name: DATA_DIRECTORY_SERVICE.to_owned(),
                            peers: self.rebind.clone(),
                            reply_to: ctx.self_addr(),
                        },
                    );
                }
            }
            _ => {}
        }
    }
}

pub(crate) struct DataNamespaceAuthority {
    service: DataNamespaceService,
    _publication_wake: Arc<dyn Fn() + Send + Sync>,
    _connection_watch: ConnectionWatch,
}

impl DataNamespaceAuthority {
    pub(crate) fn start(
        stack: &DistributionRuntimeStack,
        driver: &IrohDriver,
        state_path: impl AsRef<Path>,
        recovery_peers: Vec<swactor_transport::NodeId>,
    ) -> Result<Self, String> {
        let runtime = stack.runtime.clone();
        let source_sender: Arc<dyn BlobTransferSender> = Arc::new(IrohBlobTransferSender::new(
            driver.edge_connector(),
            &stack.engine,
            runtime.clone(),
        ));
        let publisher = Arc::new(LocalActorPublisher {
            runtime: runtime.clone(),
            distribution_directory: stack.actors.directory,
            registrar: driver.actor_registrar(),
        });
        let source_publisher: Arc<dyn BlobSourcePublisher> = publisher.clone();
        let service = DataNamespaceService::recover(
            runtime.clone(),
            stack.engine.clone(),
            state_path,
            source_sender,
            source_publisher,
            Some(Arc::new(
                crate::contextual_process::MyelinChildRouteRegistrar::new(
                    stack.route_view.clone(),
                    stack.pinned_routes.clone(),
                    stack.route_binder.clone(),
                    runtime.clone(),
                    stack.actors.directory,
                    driver.connection_observer(),
                ),
            )),
            None,
        )
        .map_err(|error| format!("recover data namespace: {error}"))?;
        let directory = service.directory();
        let service_timestamp =
            RECOVERED_SERVICE_TIMESTAMP_BASE.saturating_add(service.authority_epoch());
        publisher.publish(directory, service_timestamp)?;
        runtime
            .send_to(
                stack.actors.registry,
                RegistryIn::RegisterNameAt {
                    name: DATA_DIRECTORY_SERVICE.to_owned(),
                    actor_addr: directory,
                    timestamp: service_timestamp,
                },
            )
            .map_err(|error| format!("publish data directory service: {error}"))?;
        let service_publisher = runtime
            .spawn(NamespaceServicePublisher {
                registry: stack.actors.registry,
                view: Arc::clone(&stack.registry_view),
                directory,
                timestamp: service_timestamp,
                rebind: recovery_peers,
            })
            .map_err(|error| format!("spawn namespace service publisher: {error}"))?;
        let sender = runtime.create_sender();
        let publication_wake: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            let _ = sender.send_to(
                service_publisher,
                RegistryIn::RegisterNameAt {
                    name: DATA_DIRECTORY_SERVICE.to_owned(),
                    actor_addr: directory,
                    timestamp: service_timestamp,
                },
            );
        });
        runtime
            .send_to(
                stack.actors.registry,
                RegistryIn::WatchName {
                    name: DATA_DIRECTORY_SERVICE.to_owned(),
                    changed: Arc::downgrade(&publication_wake),
                },
            )
            .map_err(|error| format!("watch namespace publication: {error}"))?;
        let connection_watch = driver
            .connection_observer()
            .watch_connections(publication_wake.clone());
        Ok(Self {
            service,
            _publication_wake: publication_wake,
            _connection_watch: connection_watch,
        })
    }

    pub(crate) fn directory(&self) -> ActorAddress {
        self.service.directory()
    }

    pub(crate) fn control(&self) -> DataPlaneControl {
        self.service.control()
    }
}

pub(crate) struct InstalledNamespaceClient {
    pub(crate) client: NamespaceClient,
    pub(crate) source_publisher: Arc<dyn BlobSourcePublisher>,
}

pub(crate) fn install_namespace_client(
    stack: &DistributionRuntimeStack,
    driver: &IrohDriver,
) -> Result<InstalledNamespaceClient, String> {
    let connections = driver.connection_observer();
    let observed_connections = connections.clone();
    let local_node = driver.node_id();
    let discovery = Arc::new(RegistryNamespaceDiscovery {
        view: Arc::clone(&stack.registry_view),
        claims: stack.directory_claims.clone(),
        routes: Arc::clone(&stack.route_view),
        connection_watch: OnceLock::new(),
        connection_generation: Arc::new(move |host| {
            if host == local_node {
                Some(0)
            } else {
                observed_connections.generation(&host)
            }
        }),
        reply_route: NamespaceReplyRoute::new(&stack.runtime, stack.actors.directory, local_node),
    });
    let proxy = stack
        .runtime
        .spawn(NamespaceClientActor::new_event_driven(discovery.clone()))
        .map_err(|error| format!("spawn namespace client: {error}"))?;
    let proxy_claim = driver.register_actor(proxy, 1);
    discovery
        .reply_route
        .claim
        .set(proxy_claim.clone())
        .expect("namespace proxy claim installed once");
    stack.register_local_actor(proxy_claim);
    let sender = stack.runtime.create_sender();
    let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
        let _ = sender.send_to(proxy, NamespaceClientIn::Retry);
    });
    discovery
        .reply_route
        .wake
        .set(wake.clone())
        .map_err(|_| "namespace reply-route wake installed twice".to_owned())?;
    let wake = discovery.reply_route.observer(wake);
    stack
        .runtime
        .send_to(
            stack.actors.registry,
            RegistryIn::WatchName {
                name: DATA_DIRECTORY_SERVICE.to_owned(),
                changed: Arc::downgrade(&wake),
            },
        )
        .map_err(|error| format!("watch namespace binding: {error}"))?;
    stack
        .runtime
        .send_to(
            stack.actors.directory,
            DirectoryIn::WatchRoutes {
                changed: Arc::downgrade(&wake),
            },
        )
        .map_err(|error| format!("watch namespace reply routes: {error}"))?;
    discovery
        .connection_watch
        .set(connections.watch_connections(wake))
        .expect("namespace connection watch is installed once");
    let source_publisher: Arc<dyn BlobSourcePublisher> = Arc::new(LocalActorPublisher {
        runtime: stack.runtime.clone(),
        distribution_directory: stack.actors.directory,
        registrar: driver.actor_registrar(),
    });
    Ok(InstalledNamespaceClient {
        client: NamespaceClient::new(stack.runtime.clone(), proxy),
        source_publisher,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_plane::namespace::{
        DataDirectoryActor, DataDirectoryOut, NamespaceError, NamespaceRequest,
        register_namespace_codecs,
    };
    use data_plane::path::DataPath;
    use distribution::crypto::{Keypair, KeypairExt};
    use distribution::node::DistributedNodeConfig;
    use distribution::swim::actor::MembershipChanged;
    use distribution::transport_bridge::OutFrame;
    use distribution::types::{MemberState, NodeId};
    use swactor_engine::{Engine, SteppingBackend};

    fn node(id: NodeId) -> (Engine, SteppingBackend, DistributionRuntimeStack) {
        let (parts, runtime, codec, router) =
            DistributionRuntimeStack::build_runtime(register_namespace_codecs, None);
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).unwrap();
        let stack = DistributionRuntimeStack::new_from_runtime(
            runtime,
            codec,
            router,
            id,
            DistributedNodeConfig::default(),
            engine.handle(),
        );
        (engine, backend, stack)
    }

    fn step(backend: &SteppingBackend) {
        for _ in 0..4 {
            backend.step();
        }
    }

    fn deliver(
        frames: Vec<OutFrame>,
        target: &DistributionRuntimeStack,
        backend: &SteppingBackend,
    ) {
        let routes = target.actor_bridge_routes();
        for frame in frames {
            let message = target
                .codec
                .decode(&frame.type_tag, &frame.payload)
                .unwrap();
            let destination = routes.get(&frame.type_tag).copied().unwrap_or(frame.dest);
            target.runtime.deliver_raw(destination, message).unwrap();
        }
        step(backend);
    }

    fn transfer(
        source: &DistributionRuntimeStack,
        target: &DistributionRuntimeStack,
        backend: &SteppingBackend,
    ) {
        let frames = source.outbox.lock().unwrap().drain(..).collect();
        deliver(frames, target, backend);
    }

    fn publish(
        stack: &DistributionRuntimeStack,
        backend: &SteppingBackend,
        key: &Keypair,
        store: &Path,
    ) {
        let directory = DataDirectoryActor::recover(store, None, |_, _| {
            unreachable!("empty namespace has no sources to recover")
        })
        .unwrap();
        let timestamp = RECOVERED_SERVICE_TIMESTAMP_BASE + directory.authority_epoch();
        let directory = stack.runtime.spawn(directory).unwrap();
        stack.register_local_actor(key.sign_directory_entry(directory, timestamp));
        // Startup observation and recovery rebind may both publish the same
        // live authority. Its registry Lamport clock advances; its epoch does not.
        for _ in 0..2 {
            stack
                .runtime
                .send_to(
                    stack.actors.registry,
                    RegistryIn::RegisterNameAt {
                        name: DATA_DIRECTORY_SERVICE.to_owned(),
                        actor_addr: directory,
                        timestamp,
                    },
                )
                .unwrap();
        }
        step(backend);
    }

    #[test]
    fn recovery_acknowledgements_retire_only_exact_current_peer() {
        use distribution::registry::{ClusterRegistry, RegistryConfig, RegistryEntry};

        let owner = NodeId([1; 32]);
        let first = NodeId([2; 32]);
        let second = NodeId([3; 32]);
        let (_engine, backend, stack) = node(owner);
        let outbound = stack.runtime.new_inbox::<RegistryIn>().unwrap();
        let entry = RegistryEntry {
            name: DATA_DIRECTORY_SERVICE.to_owned(),
            actor_addr: ActorAddress([4; 32]),
            node_id: owner,
            timestamp: RECOVERED_SERVICE_TIMESTAMP_BASE + 2,
            generation: 2,
            tombstone: false,
        };
        let mut registry = ClusterRegistry::new(RegistryConfig::default());
        registry.merge(entry.clone());
        let publisher = stack
            .runtime
            .spawn(NamespaceServicePublisher {
                registry: *outbound.addr(),
                view: Arc::new(std::sync::RwLock::new(registry.snapshot())),
                directory: entry.actor_addr,
                timestamp: entry.timestamp,
                rebind: vec![first, second],
            })
            .unwrap();
        let observe = || {
            stack
                .runtime
                .send_to(
                    publisher,
                    RegistryIn::RegisterNameAt {
                        name: DATA_DIRECTORY_SERVICE.to_owned(),
                        actor_addr: entry.actor_addr,
                        timestamp: entry.timestamp,
                    },
                )
                .unwrap();
            step(&backend);
            outbound.try_recv()
        };
        let acknowledge = |entry: RegistryEntry, peer| {
            stack
                .runtime
                .send_to(publisher, RegistryIn::NameAcknowledged { entry, peer })
                .unwrap();
        };
        let mut stale = entry.clone();
        stale.generation -= 1;
        acknowledge(stale, first);
        let mut stale = entry.clone();
        stale.actor_addr = ActorAddress([5; 32]);
        acknowledge(stale, first);
        let mut stale = entry.clone();
        stale.timestamp -= 1;
        acknowledge(stale, first);
        let mut stale = entry.clone();
        stale.node_id = second;
        acknowledge(stale, first);
        let mut stale = entry.clone();
        stale.name = "other-service".to_owned();
        acknowledge(stale, first);
        let mut stale = entry.clone();
        stale.tombstone = true;
        acknowledge(stale, first);
        assert!(matches!(observe(),
            Some(RegistryIn::DisseminateNameTo { peers, .. }) if peers == vec![first, second]
        ));

        acknowledge(entry.clone(), first);
        acknowledge(entry.clone(), first);
        acknowledge(entry.clone(), NodeId([9; 32]));
        assert!(matches!(observe(),
            Some(RegistryIn::DisseminateNameTo { peers, .. }) if peers == vec![second]
        ));
        acknowledge(entry.clone(), second);
        assert!(
            observe().is_none(),
            "all exact peer ACKs retire directed recovery"
        );
    }

    #[test]
    fn missing_recovery_peer_remains_pending_until_acknowledged() {
        use distribution::registry::{ClusterRegistry, RegistryConfig};

        let owner = NodeId([1; 32]);
        let peer = NodeId([2; 32]);
        let (_engine, backend, stack) = node(owner);
        let outbound = stack.runtime.new_inbox::<RegistryIn>().unwrap();
        let directory = ActorAddress([4; 32]);
        let mut registry = ClusterRegistry::new(RegistryConfig::default());
        registry.register_at(DATA_DIRECTORY_SERVICE.to_owned(), directory, owner, 100, 1);
        let publisher = stack
            .runtime
            .spawn(NamespaceServicePublisher {
                registry: *outbound.addr(),
                view: Arc::new(std::sync::RwLock::new(registry.snapshot())),
                directory,
                timestamp: 100,
                rebind: vec![peer],
            })
            .unwrap();
        for _ in 0..3 {
            stack
                .runtime
                .send_to(
                    publisher,
                    RegistryIn::RegisterNameAt {
                        name: DATA_DIRECTORY_SERVICE.to_owned(),
                        actor_addr: directory,
                        timestamp: 100,
                    },
                )
                .unwrap();
            step(&backend);
            assert!(matches!(outbound.try_recv(),
                Some(RegistryIn::DisseminateNameTo { peers, .. }) if peers == vec![peer]
            ));
        }
        step(&backend);
        assert!(
            outbound.try_recv().is_none(),
            "no observations means no retry work"
        );
    }

    #[test]
    fn failed_reply_route_offer_fails_callers_without_self_retry_and_recovers_on_observation() {
        use distribution::messages::DirectoryGossip;

        struct Discovery {
            authority: AuthorityRoute,
            reply_route: NamespaceReplyRoute,
        }
        impl NamespaceDiscovery for Discovery {
            fn current_directory(&self) -> Option<ActorAddress> {
                Some(self.authority.directory)
            }
            fn prepare_reply_route(&self, _: ActorAddress) -> NamespaceRouteReadiness {
                self.reply_route.prepare(self.authority)
            }
        }
        struct RouteOffers(ActorAddress);
        impl ActorInterface for RouteOffers {
            type Incoming = DirectoryIn;
            type Response = ();

            fn handle(&mut self, ctx: &Ctx<'_>, message: DirectoryIn) {
                ctx.send(self.0, message).unwrap();
            }
        }

        let key = Keypair::from_bytes(&[7; 32]);
        let authority_key = Keypair::from_bytes(&[8; 32]);
        let (_engine, backend, stack) = node(key.node_id());
        let (_authority_engine, authority_backend, authority) = node(authority_key.node_id());
        let state = tempfile::tempdir().unwrap();
        let directory = authority
            .runtime
            .spawn(
                DataDirectoryActor::recover(state.path().join("namespace.json"), None, |_, _| {
                    Err(NamespaceError::SourceRecovery("no source".to_owned()))
                })
                .unwrap(),
            )
            .unwrap();
        for (stack, peer) in [
            (&stack, authority_key.node_id()),
            (&authority, key.node_id()),
        ] {
            stack
                .runtime
                .send_to(
                    stack.actors.directory,
                    DirectoryIn::Membership(MembershipChanged {
                        node_id: peer,
                        state: MemberState::Alive,
                        incarnation: 1,
                    }),
                )
                .unwrap();
        }
        // Discovery must identify an actual remote authority. A local directory
        // cannot exercise the outgoing RPC and installed return-route contract.
        assert!(!stack.runtime.is_local_actor(directory));
        stack
            .runtime
            .send_to(
                stack.actors.directory,
                DirectoryIn::Gossip(DirectoryGossip {
                    claims: vec![authority_key.sign_directory_entry(directory, 1)],
                }),
            )
            .unwrap();
        step(&backend);
        step(&authority_backend);
        transfer(&stack, &authority, &authority_backend);
        transfer(&authority, &stack, &backend);
        assert_eq!(stack.route_owner(directory), Some(authority_key.node_id()));
        let offers = stack.runtime.new_inbox::<DirectoryIn>().unwrap();
        // ExternalSender targets actors, not external inboxes. Observe offers
        // through an actor just like the production distribution directory.
        let route_offers = stack.runtime.spawn(RouteOffers(*offers.addr())).unwrap();
        let discovery = Arc::new(Discovery {
            authority: AuthorityRoute {
                directory,
                epoch: 1,
                host: authority_key.node_id(),
                connection_generation: 1,
            },
            reply_route: NamespaceReplyRoute::new(&stack.runtime, route_offers, key.node_id()),
        });
        let proxy = stack
            .runtime
            .spawn(NamespaceClientActor::new_event_driven(discovery.clone()))
            .unwrap();
        discovery
            .reply_route
            .claim
            .set(key.sign_directory_entry(proxy, 1))
            .unwrap();
        let sender = stack.runtime.create_sender();
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            sender.send_to(proxy, NamespaceClientIn::Retry).unwrap();
        });
        assert!(discovery.reply_route.wake.set(wake.clone()).is_ok());
        let changed = discovery.reply_route.observer(wake);
        let replies = stack.runtime.new_inbox::<DataDirectoryOut>().unwrap();
        let path = DataPath::parse("/missing").unwrap();
        let request = NamespaceClientIn::Request {
            request: NamespaceRequest::Lookup { path: path.clone() },
            reply_to: *replies.addr(),
        };

        stack.runtime.send_to(proxy, request.clone()).unwrap();
        step(&backend);
        let Some(DirectoryIn::EnsureRoute {
            completed,
            peer,
            claim,
            ..
        }) = offers.try_recv()
        else {
            panic!("initial request must offer its reply route");
        };
        assert_eq!(peer, authority_key.node_id());
        assert_eq!(claim.actor_addr, proxy);
        let completed = completed.upgrade().unwrap();
        assert!(replies.try_recv().is_none());
        completed(Err(()));
        step(&backend);
        assert!(matches!(
            replies.try_recv(),
            Some(DataDirectoryOut::LookedUp {
                result: Err(NamespaceError::DirectoryUnavailable(_)),
                ..
            })
        ));
        completed(Ok(()));
        completed(Err(()));
        step(&backend);
        assert!(
            replies.try_recv().is_none(),
            "a failed caller completes exactly once"
        );
        stack.runtime.send_to(proxy, request.clone()).unwrap();
        step(&backend);
        assert!(matches!(
            replies.try_recv(),
            Some(DataDirectoryOut::LookedUp {
                result: Err(NamespaceError::DirectoryUnavailable(_)),
                ..
            })
        ));
        assert!(
            offers.try_recv().is_none(),
            "failure and callers cannot self-retry"
        );
        assert!(replies.try_recv().is_none());
        assert!(
            stack.outbox.lock().unwrap().is_empty(),
            "failed offers cannot dispatch RPCs"
        );

        changed();
        stack.runtime.send_to(proxy, request).unwrap();
        step(&backend);
        let Some(DirectoryIn::EnsureRoute {
            completed, claim, ..
        }) = offers.try_recv()
        else {
            panic!("a later observation must permit a fresh offer");
        };
        assert!(replies.try_recv().is_none());
        authority
            .runtime
            .send_to(
                authority.actors.directory,
                DirectoryIn::Gossip(DirectoryGossip {
                    claims: vec![claim],
                }),
            )
            .unwrap();
        step(&authority_backend);
        assert_eq!(authority.route_owner(proxy), Some(key.node_id()));
        completed.upgrade().unwrap()(Ok(()));
        step(&backend);
        assert!(
            replies.try_recv().is_none(),
            "recovery must traverse the remote authority"
        );
        transfer(&stack, &authority, &authority_backend);
        transfer(&authority, &stack, &backend);
        assert!(matches!(replies.try_recv(),
            Some(DataDirectoryOut::LookedUp {
                result: Err(NamespaceError::PathNotFound(missing)), ..
            }) if missing == path
        ));
        assert!(offers.try_recv().is_none());
        assert!(
            replies.try_recv().is_none(),
            "recovery completes only the new caller"
        );
    }

    #[test]
    fn namespace_requests_wait_for_correlated_reply_route_ack_and_recovery() {
        use std::sync::atomic::{AtomicU64, Ordering};
        let authority_key = Keypair::from_bytes(&[1; 32]);
        let worker_key = Keypair::from_bytes(&[2; 32]);
        let (_authority_engine, authority_backend, authority) = node(authority_key.node_id());
        let (_worker_engine, worker_backend, worker) = node(worker_key.node_id());
        let state = tempfile::tempdir().unwrap();
        let store = state.path().join("namespace.json");
        publish(&authority, &authority_backend, &authority_key, &store);

        for (stack, peer) in [
            (&authority, worker_key.node_id()),
            (&worker, authority_key.node_id()),
        ] {
            stack
                .runtime
                .send_to(
                    stack.actors.directory,
                    DirectoryIn::Membership(MembershipChanged {
                        node_id: peer,
                        state: MemberState::Alive,
                        incarnation: 1,
                    }),
                )
                .unwrap();
        }
        let generation = Arc::new(AtomicU64::new(1));
        let observed_generation = generation.clone();
        let discovery = Arc::new(RegistryNamespaceDiscovery {
            view: worker.registry_view.clone(),
            claims: worker.directory_claims.clone(),
            routes: worker.route_view.clone(),
            connection_watch: OnceLock::new(),
            connection_generation: Arc::new(move |_| {
                Some(observed_generation.load(Ordering::SeqCst))
            }),
            reply_route: NamespaceReplyRoute::new(
                &worker.runtime,
                worker.actors.directory,
                worker_key.node_id(),
            ),
        });
        let proxy = worker
            .runtime
            .spawn(NamespaceClientActor::new_event_driven(discovery.clone()))
            .unwrap();
        let claim = worker_key.sign_directory_entry(proxy, 1);
        discovery.reply_route.claim.set(claim.clone()).unwrap();
        worker.register_local_actor(claim);
        let sender = worker.runtime.create_sender();
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            sender.send_to(proxy, NamespaceClientIn::Retry).unwrap();
        });
        assert!(discovery.reply_route.wake.set(wake.clone()).is_ok());
        let wake = discovery.reply_route.observer(wake);
        worker
            .runtime
            .send_to(
                worker.actors.registry,
                RegistryIn::WatchName {
                    name: DATA_DIRECTORY_SERVICE.to_owned(),
                    changed: Arc::downgrade(&wake),
                },
            )
            .unwrap();
        worker
            .runtime
            .send_to(
                worker.actors.directory,
                DirectoryIn::WatchRoutes {
                    changed: Arc::downgrade(&wake),
                },
            )
            .unwrap();
        let replies = worker.runtime.new_inbox::<DataDirectoryOut>().unwrap();
        let path = DataPath::parse("/cases/fresh-child/missing").unwrap();
        let request = NamespaceClientIn::Request {
            request: NamespaceRequest::Lookup { path: path.clone() },
            reply_to: *replies.addr(),
        };
        worker.runtime.send_to(proxy, request.clone()).unwrap();
        worker
            .runtime
            .send_to(
                worker.actors.directory,
                DirectoryIn::SyncTo {
                    peer: authority_key.node_id(),
                },
            )
            .unwrap();
        step(&worker_backend);
        let delayed_claims = worker.outbox.lock().unwrap().drain(..).collect();
        assert!(matches!(
            replies.try_recv(),
            Some(DataDirectoryOut::LookedUp {
                result: Err(NamespaceError::DirectoryUnavailable(_)),
                ..
            })
        ));

        authority
            .runtime
            .send_to(
                authority.actors.registry,
                RegistryIn::SyncTo {
                    peer: worker_key.node_id(),
                },
            )
            .unwrap();
        authority
            .runtime
            .send_to(
                authority.actors.directory,
                DirectoryIn::SyncTo {
                    peer: worker_key.node_id(),
                },
            )
            .unwrap();
        step(&authority_backend);
        transfer(&authority, &worker, &worker_backend);
        worker.runtime.send_to(proxy, request.clone()).unwrap();
        step(&worker_backend);
        let offers: Vec<_> = worker.outbox.lock().unwrap().drain(..).collect();
        assert!(offers.iter().all(|frame| {
            !worker
                .codec
                .decode(&frame.type_tag, &frame.payload)
                .unwrap()
                .is::<data_plane::namespace::DataDirectoryIn>()
        }));
        assert_eq!(
            authority.route_owner(proxy),
            None,
            "ordinary proxy gossip is still delayed"
        );
        assert!(
            replies.try_recv().is_none(),
            "request remains owned while reply-route installation is pending"
        );
        deliver(offers, &authority, &authority_backend);
        assert_eq!(authority.route_owner(proxy), Some(worker_key.node_id()));
        let stale_route_ack = authority.outbox.lock().unwrap().drain(..).collect();
        // Replace the connection before its acknowledgement arrives. The old
        // acknowledgement must not release the request onto the new transport.
        generation.store(2, Ordering::SeqCst);
        worker
            .runtime
            .send_to(proxy, NamespaceClientIn::Retry)
            .unwrap();
        step(&worker_backend);
        deliver(stale_route_ack, &worker, &worker_backend);
        assert!(replies.try_recv().is_none());
        assert!(worker.outbox.lock().unwrap().iter().all(|frame| {
            !worker
                .codec
                .decode(&frame.type_tag, &frame.payload)
                .unwrap()
                .is::<data_plane::namespace::DataDirectoryIn>()
        }));
        transfer(&worker, &authority, &authority_backend);
        transfer(&authority, &worker, &worker_backend);
        transfer(&worker, &authority, &authority_backend);
        transfer(&authority, &worker, &worker_backend);
        assert!(matches!(
            replies.try_recv(),
            Some(DataDirectoryOut::LookedUp {
                authority_epoch: 1,
                result: Err(NamespaceError::PathNotFound(missing)),
                ..
            }) if missing == path
        ));
        deliver(delayed_claims, &authority, &authority_backend);

        // Hold a genuine old-authority reply across recovery. Publishing only
        // the new name must not let the old reply through while its claim is
        // still undiscovered; the signed new route then wakes and re-drives it.
        worker.runtime.send_to(proxy, request.clone()).unwrap();
        step(&worker_backend);
        transfer(&worker, &authority, &authority_backend);
        let stale = authority.outbox.lock().unwrap().drain(..).collect();
        publish(&authority, &authority_backend, &authority_key, &store);
        authority
            .runtime
            .send_to(
                authority.actors.registry,
                RegistryIn::SyncTo {
                    peer: worker_key.node_id(),
                },
            )
            .unwrap();
        step(&authority_backend);
        let mut claims = Vec::new();
        let mut bindings = Vec::new();
        for frame in authority.outbox.lock().unwrap().drain(..) {
            if frame.type_tag == "swactor_dist::DirectoryGossip" {
                claims.push(frame);
            } else {
                bindings.push(frame);
            }
        }
        deliver(bindings, &worker, &worker_backend);
        deliver(stale, &worker, &worker_backend);
        assert!(
            matches!(
                replies.try_recv(),
                Some(DataDirectoryOut::LookedUp {
                    result: Err(NamespaceError::DirectoryUnavailable(_)),
                    ..
                })
            ),
            "a lost authority fails causally; the old epoch cannot complete the lookup"
        );
        deliver(claims, &worker, &worker_backend);
        authority
            .runtime
            .send_to(
                authority.actors.directory,
                DirectoryIn::SyncTo {
                    peer: worker_key.node_id(),
                },
            )
            .unwrap();
        step(&authority_backend);
        transfer(&authority, &worker, &worker_backend);
        worker.runtime.send_to(proxy, request).unwrap();
        step(&worker_backend);
        transfer(&worker, &authority, &authority_backend);
        transfer(&authority, &worker, &worker_backend);
        transfer(&worker, &authority, &authority_backend);
        transfer(&authority, &worker, &worker_backend);
        assert!(matches!(
            replies.try_recv(),
            Some(DataDirectoryOut::LookedUp {
                authority_epoch: 2,
                result: Err(NamespaceError::PathNotFound(missing)),
                ..
            }) if missing == path
        ));
        // No clock advancement, gossip tick, or namespace retry tick occurred.
    }
}
