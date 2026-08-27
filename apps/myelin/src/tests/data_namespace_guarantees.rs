#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId as ArenaNodeId};
use data_plane::blob;
use data_plane::blob_transfer::{BlobTransferReceiver, BlobTransferSender};
use data_plane::bootstrap::{self, BootstrapSpec};
use data_plane::data_plane::DataPlaneBootstrap;
use data_plane::host::{HostDataPlaneConfig, HostDataPlaneSessionActor};
use data_plane::namespace::{DirectoryClient, NamespaceError};
use data_plane::path::{DataPath, SessionAccess};
use data_plane::protocol::SessionCapability;
use iroh_driver::{IrohBlobTransferReceiver, IrohBlobTransferSender, IrohDriver};
use swactor_engine::Engine;

use crate::contextual_process::MyelinChildRouteRegistrar;
use crate::data_namespace::{
    DATA_DIRECTORY_SERVICE, DataNamespaceAuthority, InstalledNamespaceClient,
    install_namespace_client,
};
use crate::orchestration::distribution_stack::DistributionRuntimeStack;
use crate::tests::harness::build_iroh_composition;

const POLL: Duration = Duration::from_millis(25);
const DEADLINE: Duration = Duration::from_secs(30);
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
    fn start() -> Self {
        let (engine, driver, stack) = build_iroh_composition(POLL);
        let namespace = install_namespace_client(&stack, &driver).expect("namespace client");
        let receiver = Arc::new(IrohBlobTransferReceiver::new(
            driver.endpoint_addr(),
            driver.edge_events_handle(),
        ));
        receiver.install_pump(&engine.handle(), stack.runtime.clone(), POLL);
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

    fn session(
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
                    ))),
                    stream_transport: None,
                })
                .expect("host session"),
            )
            .expect("spawn host session");
        futures_lite::future::block_on(DataPlaneBootstrap::attach(
            handoff.arena_fd,
            self.stack.runtime.clone(),
            host,
            CAPABILITY,
        ))
        .expect("attach child session")
    }
}

#[test]
fn control_and_session_publications_cross_real_iroh_and_outlive_the_producer() {
    let state = tempfile::tempdir().expect("namespace state");
    let file = state.path().join("registered.bin");
    std::fs::write(&file, b"control-file-over-iroh").expect("registered file");

    let node_a = DataNode::start();
    let node_b = DataNode::start();
    let authority = DataNamespaceAuthority::start(
        &node_a.stack,
        &node_a.driver,
        state.path().join("namespace.json"),
    )
    .expect("namespace authority");

    node_a
        .driver
        .join(std::slice::from_ref(&node_b.driver.endpoint_addr()));
    assert!(wait_until(DEADLINE, || {
        namespace_visible(&node_a.stack, authority.directory())
            && namespace_visible(&node_b.stack, authority.directory())
    }));

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
    assert!(wait_until(DEADLINE, || {
        node_b.stack.route_owner(file_source).is_some()
    }));

    let reader_b = node_b.session(1, vec![DataPath::parse("/shared").unwrap()], vec![]);
    let registered = futures_lite::future::block_on(reader_b.data_plane.read_blob(&file_path))
        .expect("remote registered-file read");
    assert_eq!(
        registered.map().unwrap().as_ref(),
        b"control-file-over-iroh"
    );
    drop(registered);

    let published_path = DataPath::parse("/shared/published").unwrap();
    let producer = node_b.session(
        2,
        vec![DataPath::parse("/shared").unwrap()],
        vec![DataPath::parse("/shared").unwrap()],
    );
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

    let reader_a = node_a.session(3, vec![DataPath::parse("/shared").unwrap()], vec![]);
    let published = futures_lite::future::block_on(reader_a.data_plane.read_blob(&published_path))
        .expect("read after producer close");
    assert_eq!(published.map().unwrap().as_ref(), b"session-over-iroh");
    drop(published);

    futures_lite::future::block_on(authority.control().unregister(published_path.clone()))
        .expect("unregister publication");
    assert!(matches!(
        futures_lite::future::block_on(directory.resolve(published_path)),
        Err(NamespaceError::PathNotFound(_))
    ));
    assert!(wait_until(DEADLINE, || {
        node_b
            .stack
            .runtime
            .stats()
            .actors
            .iter()
            .all(|(actor, _)| *actor != published_source)
    }));

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

fn wait_until(deadline: Duration, mut check: impl FnMut() -> bool) -> bool {
    let started = Instant::now();
    loop {
        if check() {
            return true;
        }
        if started.elapsed() >= deadline {
            return false;
        }
        std::thread::sleep(POLL);
    }
}
