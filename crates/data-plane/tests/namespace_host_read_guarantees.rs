#![cfg(target_os = "linux")]

use std::os::unix::fs::FileExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId};
use data_plane::blob;
use data_plane::blob_transfer::{
    BlobTransferEvent, BlobTransferId, BlobTransferOffer, BlobTransferReceiver, BlobTransferSender,
    FileTransferRequest,
};
use data_plane::bootstrap::{self, BootstrapSpec};
use data_plane::control::DataNamespaceService;
use data_plane::data_plane::DataPlaneBootstrap;
use data_plane::host::{HostDataPlaneConfig, HostDataPlaneSessionActor};
use data_plane::namespace::{
    DirectoryClient, NamespaceClient, NamespaceClientActor, NamespaceDiscovery, NamespaceError,
    OperationId,
};
use data_plane::path::{DataPath, JobContext};
use data_plane::protocol::JobCapability;
use data_plane::source::{BlobSourcePublisher, FileBlobSourceActor};
use futures_lite::future;
use parking_lot::RwLock;
use swactor::Error;
use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::{RemoteSink, Runtime, RuntimeParts};
use swactor_engine::{Engine, TokioBackend, TokioConfig};

const CAPABILITY: JobCapability = JobCapability::new([0x33; 32]);
static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

struct DirectRuntimeSink {
    destination: Runtime,
}

impl RemoteSink for DirectRuntimeSink {
    fn send(
        &self,
        address: ActorAddress,
        message: Box<dyn std::any::Any + Send>,
    ) -> Result<(), Error> {
        self.destination.deliver_raw(address, message)
    }
}

struct StaticDiscovery(Arc<RwLock<Option<ActorAddress>>>);

impl NamespaceDiscovery for StaticDiscovery {
    fn current_directory(&self) -> Option<ActorAddress> {
        *self.0.read()
    }
}

struct LoopbackSender {
    runtime: Runtime,
    behavior: Arc<AtomicU8>,
}

impl BlobTransferSender for LoopbackSender {
    fn start_file(&self, request: FileTransferRequest) -> Result<(), String> {
        let mut bytes = vec![0_u8; request.length as usize];
        request
            .file
            .read_exact_at(&mut bytes, request.offset)
            .map_err(|error| error.to_string())?;
        match self.behavior.load(Ordering::Acquire) {
            1 => {
                bytes.pop();
            }
            2 => bytes.push(0xff),
            3 => return Err("injected source start failure".to_owned()),
            _ => {}
        }
        self.runtime
            .send_to(
                request.offer.destination,
                BlobTransferEvent::Chunk {
                    transfer_id: request.offer.transfer_id,
                    bytes,
                },
            )
            .map_err(|error| error.to_string())?;
        self.runtime
            .send_to(
                request.offer.destination,
                BlobTransferEvent::Finished {
                    transfer_id: request.offer.transfer_id,
                },
            )
            .map_err(|error| error.to_string())?;
        request.completion.complete(Ok(()));
        Ok(())
    }
}

struct DirectReceiver;

impl BlobTransferReceiver for DirectReceiver {
    fn open(
        &self,
        destination: ActorAddress,
        transfer_id: BlobTransferId,
    ) -> Result<BlobTransferOffer, String> {
        Ok(BlobTransferOffer {
            transfer_id,
            destination,
            failure_proxy: None,
            transport: Vec::new(),
        })
    }

    fn cancel(&self, _offer: &BlobTransferOffer) {}
}

struct NoopSourceRegistrar;

impl BlobSourcePublisher for NoopSourceRegistrar {
    fn publish_source(&self, _source: ActorAddress) -> Result<(), String> {
        Ok(())
    }
}

struct TempState {
    root: PathBuf,
}

impl TempState {
    fn new() -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "swactor-host-namespace-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self { root }
    }
}

impl Drop for TempState {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn runtime_parts() -> (RuntimeParts, Runtime) {
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    (parts, runtime)
}

#[test]
fn host_read_resolves_file_source_and_seals_final_arena_lease() {
    let state = TempState::new();
    let weights = b"namespace-backed-weights";
    let source_path = state.root.join("weights.bin");
    std::fs::write(&source_path, weights).unwrap();
    let store_path = state.root.join("namespace.json");

    let mut arena = ArenaManager::boot(ArenaConfig {
        node_id: NodeId(1),
        reservation_ceiling: 4096,
        base_alignment: 64,
    })
    .unwrap();
    let handoff = bootstrap::write_bootstrap(
        &mut arena,
        BootstrapSpec {
            arena_generation: 1,
            alignment: 64,
        },
    )
    .unwrap();
    let mut consumer_arena = ArenaManager::boot(ArenaConfig {
        node_id: NodeId(2),
        reservation_ceiling: 4096,
        base_alignment: 64,
    })
    .unwrap();
    let consumer_handoff = bootstrap::write_bootstrap(
        &mut consumer_arena,
        BootstrapSpec {
            arena_generation: 2,
            alignment: 64,
        },
    )
    .unwrap();
    let (host_parts, host_runtime) = runtime_parts();
    let (child_parts, child_runtime) = runtime_parts();
    host_runtime.set_remote_sink(Arc::new(DirectRuntimeSink {
        destination: child_runtime.clone(),
    }));
    child_runtime.set_remote_sink(Arc::new(DirectRuntimeSink {
        destination: host_runtime.clone(),
    }));
    let host_engine = Engine::new(
        host_parts,
        TokioBackend::new(TokioConfig::default()).unwrap(),
    )
    .unwrap();
    let child_engine = Engine::new(
        child_parts,
        TokioBackend::new(TokioConfig::default()).unwrap(),
    )
    .unwrap();

    let transfer_behavior = Arc::new(AtomicU8::new(0));
    let sender: Arc<dyn BlobTransferSender> = Arc::new(LoopbackSender {
        runtime: host_runtime.clone(),
        behavior: Arc::clone(&transfer_behavior),
    });
    let source_publisher: Arc<dyn BlobSourcePublisher> = Arc::new(NoopSourceRegistrar);
    let service = DataNamespaceService::recover(
        host_runtime.clone(),
        &store_path,
        Arc::clone(&sender),
        Arc::clone(&source_publisher),
    )
    .unwrap();
    let directory = service.directory();
    let direct = DirectoryClient::new(host_runtime.clone(), directory);
    let logical = DataPath::parse("/models/weights").unwrap();
    future::block_on(
        service
            .control()
            .register(logical.clone(), blob::file(&source_path)),
    )
    .unwrap();
    let discovered = Arc::new(RwLock::new(Some(directory)));
    let proxy = host_runtime
        .spawn(NamespaceClientActor::new(
            host_engine.handle(),
            host_runtime.create_sender(),
            Arc::new(StaticDiscovery(Arc::clone(&discovered))),
            Duration::from_millis(5),
        ))
        .unwrap();
    let namespace = NamespaceClient::new(host_runtime.clone(), proxy);
    let host_session = host_runtime
        .spawn(
            HostDataPlaneSessionActor::new(HostDataPlaneConfig {
                runtime: host_runtime.clone(),
                arena,
                arena_generation: 1,
                session_generation: 1,
                capability: CAPABILITY,
                job_context: JobContext {
                    run_id: "run-1".to_owned(),
                    read_prefixes: vec![
                        DataPath::parse("/models").unwrap(),
                        DataPath::parse("/runs/run-1").unwrap(),
                    ],
                    write_prefixes: vec![DataPath::parse("/runs/run-1").unwrap()],
                },
                namespace: Some(namespace.clone()),
                transfer_receiver: Some(Arc::new(DirectReceiver)),
                source_sender: Some(Arc::clone(&sender)),
                source_publisher: Some(Arc::clone(&source_publisher)),
                route_registrar: None,
            })
            .unwrap(),
        )
        .unwrap();
    let bootstrap = future::block_on(DataPlaneBootstrap::attach(
        handoff.arena_fd,
        child_runtime.clone(),
        host_session,
        CAPABILITY,
    ))
    .unwrap();
    let consumer_host_session = host_runtime
        .spawn(
            HostDataPlaneSessionActor::new(HostDataPlaneConfig {
                runtime: host_runtime.clone(),
                arena: consumer_arena,
                arena_generation: 2,
                session_generation: 2,
                capability: CAPABILITY,
                job_context: JobContext {
                    run_id: "run-1".to_owned(),
                    read_prefixes: vec![
                        DataPath::parse("/models").unwrap(),
                        DataPath::parse("/runs/run-1").unwrap(),
                    ],
                    write_prefixes: vec![],
                },
                namespace: Some(namespace),
                transfer_receiver: Some(Arc::new(DirectReceiver)),
                source_sender: Some(Arc::clone(&sender)),
                source_publisher: Some(source_publisher),
                route_registrar: None,
            })
            .unwrap(),
        )
        .unwrap();
    let consumer = future::block_on(DataPlaneBootstrap::attach(
        consumer_handoff.arena_fd,
        child_runtime,
        consumer_host_session,
        CAPABILITY,
    ))
    .unwrap();
    let blob = future::block_on(bootstrap.data_plane.read_blob(&logical)).unwrap();

    for (mode, name) in [(1_u8, "short"), (2, "oversized"), (3, "start-failure")] {
        let fault_path = DataPath::parse(format!("/models/{name}")).unwrap();
        let source =
            FileBlobSourceActor::open(host_runtime.clone(), Arc::clone(&sender), &source_path)
                .unwrap();
        let length = source.length();
        let recovery = source.recovery();
        let source = host_runtime.spawn(source).unwrap();
        future::block_on(direct.register(
            fault_path.clone(),
            source,
            length,
            recovery,
            OperationId::from_u128(10 + u128::from(mode)),
        ))
        .unwrap();
        transfer_behavior.store(mode, Ordering::Release);
        let result = future::block_on(bootstrap.data_plane.read_blob(&fault_path));
        match mode {
            1 | 2 => assert!(matches!(
                result,
                Err(data_plane::protocol::DataPlaneError::Blob(
                    data_plane::protocol::BlobFailure::Length { .. }
                ))
            )),
            3 => assert!(matches!(
                result,
                Err(data_plane::protocol::DataPlaneError::SourceFailure(_))
            )),
            _ => unreachable!(),
        }
    }
    transfer_behavior.store(0, Ordering::Release);
    let recovered_after_failures =
        future::block_on(bootstrap.data_plane.read_blob(&logical)).unwrap();
    assert_eq!(recovered_after_failures.map().unwrap().as_ref(), weights);
    drop(recovered_after_failures);
    assert_eq!(blob.length(), weights.len() as u64);

    let result_path = DataPath::parse("/runs/run-1/result").unwrap();
    let mut writer = future::block_on(
        bootstrap
            .data_plane
            .write_blob(&result_path, b"first!".len() as u64),
    )
    .unwrap();
    writer.map().unwrap().as_mut().copy_from_slice(b"first!");
    future::block_on(writer.seal()).unwrap();
    let first = future::block_on(bootstrap.data_plane.read_blob(&result_path)).unwrap();
    assert_eq!(first.map().unwrap().as_ref(), b"first!");

    let mut replacement = future::block_on(
        bootstrap
            .data_plane
            .write_blob(&result_path, b"second".len() as u64),
    )
    .unwrap();
    replacement
        .map()
        .unwrap()
        .as_mut()
        .copy_from_slice(b"second");
    future::block_on(replacement.seal()).unwrap();
    let second = future::block_on(bootstrap.data_plane.read_blob(&result_path)).unwrap();
    assert_eq!(second.map().unwrap().as_ref(), b"second");
    assert_eq!(first.map().unwrap().as_ref(), b"first!");

    let mut aborted = future::block_on(bootstrap.data_plane.write_blob(&result_path, 5)).unwrap();
    aborted.map().unwrap().as_mut().copy_from_slice(b"abort");
    future::block_on(aborted.abort()).unwrap();
    let after_abort = future::block_on(bootstrap.data_plane.read_blob(&result_path)).unwrap();
    assert_eq!(after_abort.map().unwrap().as_ref(), b"second");
    assert_eq!(blob.map().unwrap().as_ref(), weights);

    let cancelled_path = DataPath::parse("/runs/run-1/cancelled").unwrap();
    let mut cancelled =
        future::block_on(bootstrap.data_plane.write_blob(&cancelled_path, 6)).unwrap();
    cancelled.map().unwrap().as_mut().copy_from_slice(b"cancel");
    *discovered.write() = None;
    let mut sealing = Box::pin(cancelled.seal());
    assert!(
        future::block_on(future::poll_once(sealing.as_mut())).is_none(),
        "seal must wait while namespace authority is absent"
    );
    drop(sealing);
    drop(cancelled);
    *discovered.write() = Some(directory);
    host_runtime
        .send_to(proxy, data_plane::namespace::NamespaceClientIn::Retry)
        .unwrap();
    std::thread::sleep(Duration::from_millis(100));
    assert!(matches!(
        future::block_on(direct.resolve(cancelled_path)),
        Err(NamespaceError::PathNotFound(_))
    ));

    *discovered.write() = None;
    let mut cancelled_read = Box::pin(bootstrap.data_plane.read_blob(&logical));
    assert!(
        future::block_on(future::poll_once(cancelled_read.as_mut())).is_none(),
        "read must wait while namespace authority is absent"
    );
    drop(cancelled_read);
    *discovered.write() = Some(directory);
    host_runtime
        .send_to(proxy, data_plane::namespace::NamespaceClientIn::Retry)
        .unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let after_cancel = future::block_on(bootstrap.data_plane.read_blob(&logical)).unwrap();
    assert_eq!(after_cancel.map().unwrap().as_ref(), weights);
    drop(after_cancel);

    drop(after_abort);
    drop(second);
    drop(first);
    drop(blob);
    let reclaim_path = DataPath::parse("/runs/run-1/reclaim").unwrap();
    let replacement_path = DataPath::parse("/runs/run-1/reclaimed").unwrap();
    let reclaim_bytes = vec![0x5a; 3_000];
    let mut reclaiming = future::block_on(
        bootstrap
            .data_plane
            .write_blob(&reclaim_path, reclaim_bytes.len() as u64),
    )
    .unwrap();
    reclaiming
        .map()
        .unwrap()
        .as_mut()
        .copy_from_slice(&reclaim_bytes);
    future::block_on(reclaiming.seal()).unwrap();
    let reclaim_source = future::block_on(direct.resolve(reclaim_path.clone()))
        .unwrap()
        .source;
    assert!(matches!(
        future::block_on(
            bootstrap
                .data_plane
                .write_blob(&replacement_path, reclaim_bytes.len() as u64)
        ),
        Err(data_plane::protocol::DataPlaneError::ArenaExhausted)
    ));
    future::block_on(direct.unregister(reclaim_path, OperationId::from_u128(101))).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let mut replacement = loop {
        match future::block_on(
            bootstrap
                .data_plane
                .write_blob(&replacement_path, reclaim_bytes.len() as u64),
        ) {
            Ok(writer) => break writer,
            Err(data_plane::protocol::DataPlaneError::ArenaExhausted)
                if Instant::now() < deadline =>
            {
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(error) => panic!("unregister did not reclaim the published lease: {error}"),
        }
    };
    assert!(
        host_runtime
            .stats()
            .actors
            .iter()
            .all(|(actor, _)| *actor != reclaim_source),
        "unregister did not reclaim the published source actor"
    );
    replacement
        .map()
        .unwrap()
        .as_mut()
        .copy_from_slice(&reclaim_bytes);
    future::block_on(replacement.abort()).unwrap();
    let published_source = future::block_on(direct.resolve(result_path.clone()))
        .unwrap()
        .source;
    bootstrap.data_plane.close().unwrap();

    let after_producer_close =
        future::block_on(consumer.data_plane.read_blob(&result_path)).unwrap();
    assert_eq!(
        after_producer_close.map().unwrap().as_ref(),
        b"second",
        "a committed publication must outlive its producer session"
    );
    drop(after_producer_close);

    future::block_on(direct.unregister(result_path.clone(), OperationId::from_u128(100))).unwrap();
    assert!(matches!(
        future::block_on(direct.resolve(result_path)),
        Err(NamespaceError::PathNotFound(_))
    ));
    let deadline = Instant::now() + Duration::from_secs(2);
    while host_runtime
        .stats()
        .actors
        .iter()
        .any(|(actor, _)| *actor == published_source)
    {
        assert!(
            Instant::now() < deadline,
            "unregister did not reclaim the published source actor"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    consumer.data_plane.close().unwrap();

    drop(bootstrap);
    drop(child_engine);
    drop(host_engine);
}
