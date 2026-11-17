#![cfg(target_os = "linux")]

use std::sync::Arc;

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId as ArenaNodeId};
use data_plane::blob;
use data_plane::blob_transfer::{BlobTransferReceiver, BlobTransferSender};
use data_plane::bootstrap::{self, BootstrapSpec};
use data_plane::data_plane::DataPlaneBootstrap;
use data_plane::host::{HostDataPlaneConfig, HostDataPlaneSessionActor};
use data_plane::namespace::{DirectoryClient, NamespaceError};
use data_plane::path::{DataPath, SessionAccess};
use data_plane::protocol::SessionCapability;
use distribution::directory_actor::DirectoryIn;
use distribution::registry_actor::RegistryIn;
use iroh_driver::{IrohBlobTransferReceiver, IrohBlobTransferSender, IrohDriver};
use swactor::actor::{ActorAddress, ActorExited, ActorInterface, Ctx};
use swactor::std::CtxWatching;
use swactor_engine::{Engine, TaskSpawner, TokioBackend, TokioConfig};

use crate::contextual_process::MyelinChildRouteRegistrar;
use crate::data_namespace::{
    DATA_DIRECTORY_SERVICE, DataNamespaceAuthority, InstalledNamespaceClient,
    install_namespace_client,
};
use crate::orchestration::distribution_stack::DistributionRuntimeStack;
use crate::tests::harness::prepare_iroh_composition;

const CAPABILITY: SessionCapability = SessionCapability::new([0x77; 32]);

struct DataNode {
    namespace: InstalledNamespaceClient,
    receiver: Arc<dyn BlobTransferReceiver>,
    sender: Arc<dyn BlobTransferSender>,
    stack: DistributionRuntimeStack,
    driver: IrohDriver,
    _engine: Engine,
}

impl DataNode {
    fn start(
        engine: Engine,
        tasks: &TaskSpawner,
        driver: IrohDriver,
        stack: DistributionRuntimeStack,
    ) -> Self {
        let namespace = install_namespace_client(&stack, &driver).expect("namespace client");
        let receiver = Arc::new(IrohBlobTransferReceiver::new(
            driver.endpoint_addr(),
            driver.edge_events_handle(),
        ));
        receiver.install_pump(
            tasks,
            driver.connection_changes(),
            stack.runtime.clone(),
            driver.edge_events_changed(),
        );
        let receiver: Arc<dyn BlobTransferReceiver> = receiver;
        let sender: Arc<dyn BlobTransferSender> = Arc::new(IrohBlobTransferSender::new(
            driver.edge_connector(),
            &engine.handle(),
            stack.runtime.clone(),
        ));
        Self {
            namespace,
            receiver,
            sender,
            stack,
            driver,
            _engine: engine,
        }
    }

    async fn session(
        &self,
        generation: u64,
        read_prefixes: Vec<DataPath>,
        write_prefixes: Vec<DataPath>,
    ) -> DataPlaneBootstrap {
        let mut arena = ArenaManager::boot(ArenaConfig {
            node_id: ArenaNodeId(generation),
            reservation_ceiling: 64 * 1024,
            base_alignment: 64,
        })
        .expect("session arena");
        let handoff = bootstrap::prepare_arena(
            &mut arena,
            BootstrapSpec {
                arena_generation: generation,
                alignment: 64,
            },
        )
        .expect("session bootstrap");
        let host = self
            .stack
            .runtime
            .spawn(
                HostDataPlaneSessionActor::new(HostDataPlaneConfig {
                    runtime: self.stack.runtime.clone(),
                    engine: self._engine.handle(),
                    arena,
                    arena_generation: generation,
                    session_generation: generation,
                    capability: CAPABILITY,
                    session_access: SessionAccess {
                        execution_id: format!("run-{generation}"),
                        read_prefixes,
                        write_prefixes,
                    },
                    namespace: Some(self.namespace.client.clone()),
                    transfer_receiver: Some(Arc::clone(&self.receiver)),
                    source_sender: Some(Arc::clone(&self.sender)),
                    source_publisher: Some(Arc::clone(&self.namespace.source_publisher)),
                    route_registrar: Some(Arc::new(MyelinChildRouteRegistrar::new(
                        self.stack.route_view.clone(),
                        self.stack.pinned_routes.clone(),
                        self.stack.route_binder.clone(),
                        self.stack.runtime.clone(),
                        self.stack.actors.directory,
                        self.driver.connection_observer(),
                    ))),
                    stream_transport: None,
                    transitions: None,
                })
                .expect("host session"),
            )
            .expect("spawn host session");
        DataPlaneBootstrap::attach(
            handoff.arena_fd,
            self.stack.runtime.clone(),
            host,
            CAPABILITY,
            None,
        )
        .await
        .expect("attach child session")
    }
}

struct SourceExitObserver {
    watching: ActorAddress,
    exited: ActorAddress,
}

impl ActorInterface for SourceExitObserver {
    type Incoming = ActorAddress;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, source: ActorAddress) {
        ctx.watch(source);
        ctx.send(self.watching, source)
            .expect("source watch installed");
    }

    fn on_actor_exit(&mut self, ctx: &Ctx, exited: ActorExited) {
        ctx.send(self.exited, exited).expect("source exit observed");
        ctx.stop_self();
    }
}

fn observe_distribution(
    stack: &DistributionRuntimeStack,
) -> (
    Arc<dyn Fn() + Send + Sync>,
    tokio::sync::watch::Receiver<()>,
) {
    let (changed, observations) = tokio::sync::watch::channel(());
    let watch: Arc<dyn Fn() + Send + Sync> = Arc::new(move || {
        changed.send_replace(());
    });
    stack
        .runtime
        .send_to(
            stack.actors.registry,
            RegistryIn::WatchName {
                name: DATA_DIRECTORY_SERVICE.to_owned(),
                changed: Arc::downgrade(&watch),
            },
        )
        .expect("observe namespace service binding");
    stack
        .runtime
        .send_to(
            stack.actors.directory,
            DirectoryIn::WatchRoutes {
                changed: Arc::downgrade(&watch),
            },
        )
        .expect("observe namespace actor routes");
    (watch, observations)
}

#[test]
fn control_and_session_publications_cross_real_iroh_and_outlive_the_producer() {
    let state = tempfile::tempdir().expect("namespace state");
    let file = state.path().join("registered.bin");
    std::fs::write(&file, b"control-file-over-iroh").expect("registered file");

    let (parts_a, prepare_a) = prepare_iroh_composition();
    let engine_a =
        Engine::new(parts_a, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let tasks_a = engine_a.task_spawner();
    let (driver_a, stack_a) = prepare_a(tasks_a.clone(), engine_a.handle());
    let node_a = DataNode::start(engine_a, &tasks_a, driver_a, stack_a);
    let (parts_b, prepare_b) = prepare_iroh_composition();
    let engine_b =
        Engine::new(parts_b, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let tasks_b = engine_b.task_spawner();
    let (driver_b, stack_b) = prepare_b(tasks_b.clone(), engine_b.handle());
    let node_b = DataNode::start(engine_b, &tasks_b, driver_b, stack_b);
    let authority = DataNamespaceAuthority::start(
        &node_a.stack,
        &node_a.driver,
        state.path().join("namespace.json"),
        Vec::new(),
    )
    .expect("namespace authority");

    let (_watch_a, mut changed_a) = observe_distribution(&node_a.stack);
    let (_watch_b, mut changed_b) = observe_distribution(&node_b.stack);

    node_a
        .driver
        .join(std::slice::from_ref(&node_b.driver.endpoint_addr()));
    futures_lite::future::block_on(async {
        while !namespace_visible(&node_a.stack, authority.directory()) {
            changed_a
                .changed()
                .await
                .expect("node A distribution observation");
        }
        while !namespace_visible(&node_b.stack, authority.directory()) {
            changed_b
                .changed()
                .await
                .expect("node B distribution observation");
        }
    });

    let file_path = DataPath::parse("/shared/registered").unwrap();
    futures_lite::future::block_on(
        authority
            .control()
            .register(file_path.clone(), blob::file(&file)),
    )
    .expect("public file registration");
    let directory = DirectoryClient::new(node_a.stack.runtime.clone(), authority.directory());
    let file_source = futures_lite::future::block_on(directory.resolve(file_path.clone()))
        .expect("registered binding")
        .source;
    futures_lite::future::block_on(async {
        while node_b.stack.route_owner(file_source).is_none() {
            changed_b
                .changed()
                .await
                .expect("registered source route observation");
        }
    });

    let reader_b = futures_lite::future::block_on(node_b.session(
        1,
        vec![DataPath::parse("/shared").unwrap()],
        vec![],
    ));
    let registered = futures_lite::future::block_on(reader_b.data_plane.read_blob(&file_path))
        .expect("remote registered-file read");
    assert_eq!(
        registered.map().unwrap().as_ref(),
        b"control-file-over-iroh"
    );
    drop(registered);

    let published_path = DataPath::parse("/shared/published").unwrap();
    let producer = futures_lite::future::block_on(node_b.session(
        2,
        vec![DataPath::parse("/shared").unwrap()],
        vec![DataPath::parse("/shared").unwrap()],
    ));
    let mut writer = futures_lite::future::block_on(
        producer
            .data_plane
            .write_blob(&published_path, b"session-over-iroh".len() as u64),
    )
    .expect("open publication");
    writer
        .map()
        .unwrap()
        .as_mut()
        .copy_from_slice(b"session-over-iroh");
    futures_lite::future::block_on(writer.seal()).expect("commit publication");

    let published_source =
        futures_lite::future::block_on(directory.resolve(published_path.clone()))
            .expect("published binding")
            .source;
    producer.data_plane.close().expect("close producer");

    let reader_a = futures_lite::future::block_on(node_a.session(
        3,
        vec![DataPath::parse("/shared").unwrap()],
        vec![],
    ));
    let published = futures_lite::future::block_on(reader_a.data_plane.read_blob(&published_path))
        .expect("read after producer close");
    assert_eq!(published.map().unwrap().as_ref(), b"session-over-iroh");
    drop(published);

    let watching = node_b.stack.runtime.new_inbox::<ActorAddress>().unwrap();
    let exited = node_b.stack.runtime.new_inbox::<ActorExited>().unwrap();
    let observer = node_b
        .stack
        .runtime
        .spawn(SourceExitObserver {
            watching: *watching.addr(),
            exited: *exited.addr(),
        })
        .expect("observe retained source lifecycle");
    node_b
        .stack
        .runtime
        .send_to(observer, published_source)
        .expect("watch retained source");
    assert_eq!(
        futures_lite::future::block_on(watching.recv()),
        published_source,
    );

    futures_lite::future::block_on(authority.control().unregister(published_path.clone()))
        .expect("unregister publication");
    assert!(matches!(
        futures_lite::future::block_on(directory.resolve(published_path)),
        Err(NamespaceError::PathNotFound(_))
    ));
    assert_eq!(
        futures_lite::future::block_on(exited.recv()).addr,
        published_source,
        "unregistered publication source must retire",
    );

    reader_a.data_plane.close().expect("close reader A");
    reader_b.data_plane.close().expect("close reader B");
}

fn namespace_visible(
    stack: &DistributionRuntimeStack,
    directory: swactor::actor::ActorAddress,
) -> bool {
    stack.route_owner(directory).is_some()
        && stack
            .registry_view
            .read()
            .expect("registry view")
            .entries
            .iter()
            .any(|entry| {
                entry.name == DATA_DIRECTORY_SERVICE
                    && entry.actor_addr == directory
                    && !entry.tombstone
            })
}
