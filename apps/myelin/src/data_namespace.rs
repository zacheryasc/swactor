use std::path::Path;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use data_plane::blob_transfer::BlobTransferSender;
use data_plane::control::{DataNamespaceService, DataPlaneControl};
use data_plane::namespace::{
    NamespaceClient, NamespaceClientActor, NamespaceClientIn, NamespaceDiscovery,
};
use data_plane::source::BlobSourcePublisher;
use distribution::directory_actor::{DirectoryClaims, DirectoryIn};
use distribution::registry_actor::{RegistryIn, RegistryView};
use distribution::transport_bridge::RouteView;
use iroh_driver::{ActorRegistrar, ConnectionWatch, IrohBlobTransferSender, IrohDriver};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::Runtime;

use crate::orchestration::distribution_stack::DistributionRuntimeStack;

pub(crate) const DATA_DIRECTORY_SERVICE: &str = "swactor.data-directory";
const NAMESPACE_RETRY: Duration = Duration::from_secs(1);
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
struct RegistryNamespaceDiscovery {
    view: RegistryView,
    claims: DirectoryClaims,
    routes: RouteView,
    connection_watch: OnceLock<ConnectionWatch>,
}

impl RegistryNamespaceDiscovery {
    fn current_authority(&self) -> Option<(ActorAddress, u64)> {
        let view = self.view.read().expect("registry view poisoned");
        let binding = view
            .entries
            .iter()
            .find(|entry| entry.name == DATA_DIRECTORY_SERVICE && !entry.tombstone)?;
        let (host, generation) = self.claims.location(&binding.actor_addr)?;
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
        Some((binding.actor_addr, epoch))
    }
}

impl NamespaceDiscovery for RegistryNamespaceDiscovery {
    fn current_directory(&self) -> Option<ActorAddress> {
        self.current_authority().map(|(directory, _)| directory)
    }

    fn accepts_authority_epoch(&self, epoch: u64) -> bool {
        self.current_authority()
            .is_some_and(|(_, expected)| epoch == expected)
    }
}

/// How many publisher ticks (1 s each) a recovery re-bind keeps pushing the
/// fresh directory binding directly to unacknowledged persisted workers.
/// The ceiling is unchanged: successful peers retire on their exact ACK,
/// while missing peers retain the full dial/recovery fallback budget.
const RECOVERY_REBIND_TICKS: u32 = 120;

struct NamespaceServicePublisher {
    registry: ActorAddress,
    view: RegistryView,
    directory: ActorAddress,
    timestamp: u64,
    /// Directed recovery re-bind state: persisted peers still to re-bind and
    /// the remaining bounded tick budget.
    rebind: Option<(Vec<swactor_transport::NodeId>, u32)>,
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
                if current && let Some((peers, _)) = &mut self.rebind {
                    peers.retain(|pending| *pending != peer);
                    if peers.is_empty() {
                        self.rebind = None;
                    }
                }
            }
            RegistryIn::Tick => {
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
                // Recovery re-bind: keep pushing the fresh binding directly
                // to unacknowledged workers until the bounded budget runs out.
                // Directed gossip replaces the dead pre-crash binding on a
                // worker within one round trip instead of one SWIM
                // convergence period.
                if let Some((peers, budget)) = self.rebind.take() {
                    if !peers.is_empty() {
                        let _ = ctx.send(
                            self.registry,
                            RegistryIn::DisseminateNameTo {
                                name: DATA_DIRECTORY_SERVICE.to_owned(),
                                peers: peers.clone(),
                                reply_to: ctx.self_addr(),
                            },
                        );
                    }
                    if budget > 1 {
                        self.rebind = Some((peers, budget - 1));
                    }
                }
            }
            _ => {}
        }
    }
}

pub(crate) struct DataNamespaceAuthority {
    service: DataNamespaceService,
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
                rebind: (!recovery_peers.is_empty())
                    .then(|| (recovery_peers, RECOVERY_REBIND_TICKS)),
            })
            .map_err(|error| format!("spawn namespace service publisher: {error}"))?;
        runtime
            .send_to(service_publisher, RegistryIn::Tick)
            .map_err(|error| format!("start namespace recovery rebind: {error}"))?;
        stack.engine.send_every(
            Duration::from_secs(1),
            runtime.create_sender(),
            service_publisher,
            RegistryIn::Tick,
        );
        Ok(Self { service })
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
    let discovery = Arc::new(RegistryNamespaceDiscovery {
        view: Arc::clone(&stack.registry_view),
        claims: stack.directory_claims.clone(),
        routes: Arc::clone(&stack.route_view),
        connection_watch: OnceLock::new(),
    });
    let proxy = stack
        .runtime
        .spawn(NamespaceClientActor::new(
            stack.engine.clone(),
            stack.runtime.create_sender(),
            discovery.clone(),
            NAMESPACE_RETRY,
        ))
        .map_err(|error| format!("spawn namespace client: {error}"))?;
    stack.register_local_actor(driver.register_actor(proxy, 1));
    let sender = stack.runtime.create_sender();
    let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
        let _ = sender.send_to(proxy, NamespaceClientIn::Retry);
    });
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
        .set(driver.connection_observer().watch_connections(wake))
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
                rebind: Some((vec![first, second], RECOVERY_REBIND_TICKS)),
            })
            .unwrap();
        let tick = || {
            stack.runtime.send_to(publisher, RegistryIn::Tick).unwrap();
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
        assert!(matches!(tick(),
            Some(RegistryIn::DisseminateNameTo { peers, .. }) if peers == vec![first, second]
        ));

        acknowledge(entry.clone(), first);
        acknowledge(entry.clone(), first);
        acknowledge(entry.clone(), NodeId([9; 32]));
        assert!(matches!(tick(),
            Some(RegistryIn::DisseminateNameTo { peers, .. }) if peers == vec![second]
        ));
        acknowledge(entry, second);
        assert!(
            tick().is_none(),
            "all exact peer ACKs retire directed recovery"
        );
    }

    #[test]
    fn missing_recovery_peer_keeps_bounded_retry_budget() {
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
                rebind: Some((vec![peer], RECOVERY_REBIND_TICKS)),
            })
            .unwrap();
        for _ in 0..RECOVERY_REBIND_TICKS {
            stack.runtime.send_to(publisher, RegistryIn::Tick).unwrap();
            step(&backend);
            assert!(matches!(outbound.try_recv(),
                Some(RegistryIn::DisseminateNameTo { peers, .. }) if peers == vec![peer]
            ));
        }
        stack.runtime.send_to(publisher, RegistryIn::Tick).unwrap();
        step(&backend);
        assert!(
            outbound.try_recv().is_none(),
            "missing peers cannot push forever"
        );
    }

    #[test]
    fn namespace_lookup_uses_signed_epoch_after_republish_and_recovery_without_ticks() {
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
        let discovery = Arc::new(RegistryNamespaceDiscovery {
            view: worker.registry_view.clone(),
            claims: worker.directory_claims.clone(),
            routes: worker.route_view.clone(),
            connection_watch: OnceLock::new(),
        });
        let proxy = worker
            .runtime
            .spawn(NamespaceClientActor::new(
                worker.engine.clone(),
                worker.runtime.create_sender(),
                discovery,
                NAMESPACE_RETRY,
            ))
            .unwrap();
        worker.register_local_actor(worker_key.sign_directory_entry(proxy, 1));
        let sender = worker.runtime.create_sender();
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
            sender.send_to(proxy, NamespaceClientIn::Retry).unwrap();
        });
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
        transfer(&worker, &authority, &authority_backend);
        assert!(replies.try_recv().is_none());

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

        // Hold a genuine old-authority reply across recovery. Publishing only
        // the new name must not let the old reply through while its claim is
        // still undiscovered; the signed new route then wakes and re-drives it.
        worker.runtime.send_to(proxy, request).unwrap();
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
        transfer(&authority, &worker, &worker_backend);
        deliver(stale, &worker, &worker_backend);
        assert!(
            replies.try_recv().is_none(),
            "old epoch must not complete the lookup"
        );
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
