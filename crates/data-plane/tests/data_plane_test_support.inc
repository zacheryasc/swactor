use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId};
use data_plane::bootstrap::{self, BootstrapSpec};
use data_plane::data_plane::{
    DataPlaneBootstrap, MapRequest, MapTarget, Protection, Sharing, TransferRoute,
};
use data_plane::host::{HostDataPlaneConfig, HostDataPlaneSessionActor};
use data_plane::path::{DataPath, SessionAccess};
use data_plane::protocol::{AccessMode, DataPlaneError, Errno, OpenOptions, SessionCapability};
use futures_lite::future;
use swactor::Error;
use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::{RemoteSink, Runtime, RuntimeParts};
use swactor_engine::{Engine, TokioBackend, TokioConfig};

const CAPABILITY: SessionCapability = SessionCapability::new([9; 32]);
const ARENA_GENERATION: u64 = 17;
const SESSION_GENERATION: u64 = 29;
const WEIGHTS: &[u8] = b"0123456789abcdefghijklmn";
static STREAM_TEST_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());

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


struct Harness {
    _host_engine: Engine,
    _child_engine: Engine,
    _temp: TempState,
    _host_runtime: Runtime,
    bootstrap: DataPlaneBootstrap,
}

static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

struct TempState {
    root: std::path::PathBuf,
}

impl TempState {
    fn new() -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "swactor-actor-blob-{}-{sequence}",
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

fn path(value: &str) -> DataPath {
    DataPath::parse(value).expect("test path")
}

fn runtime_parts() -> (RuntimeParts, Runtime) {
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    (parts, runtime)
}

struct LoopbackSender {
    runtime: Runtime,
}

impl data_plane::blob_transfer::BlobTransferSender for LoopbackSender {
    fn start_file(
        &self,
        request: data_plane::blob_transfer::FileTransferRequest,
    ) -> Result<(), String> {
        use std::os::unix::fs::FileExt;
        let mut bytes = vec![0_u8; request.length as usize];
        request
            .file
            .read_exact_at(&mut bytes, request.offset)
            .map_err(|error| error.to_string())?;
        self.runtime
            .send_to(
                request.offer.destination,
                data_plane::blob_transfer::BlobTransferEvent::Chunk {
                    transfer_id: request.offer.transfer_id,
                    bytes,
                },
            )
            .map_err(|error| error.to_string())?;
        self.runtime
            .send_to(
                request.offer.destination,
                data_plane::blob_transfer::BlobTransferEvent::Finished {
                    transfer_id: request.offer.transfer_id,
                },
            )
            .map_err(|error| error.to_string())?;
        request.completion.complete(Ok(()));
        Ok(())
    }
}

struct DirectReceiver;

impl data_plane::blob_transfer::BlobTransferReceiver for DirectReceiver {
    fn open(
        &self,
        destination: ActorAddress,
        transfer_id: data_plane::blob_transfer::BlobTransferId,
    ) -> Result<data_plane::blob_transfer::BlobTransferOffer, String> {
        Ok(data_plane::blob_transfer::BlobTransferOffer {
            transfer_id,
            destination,
            failure_proxy: None,
            transport: Vec::new(),
        })
    }

    fn cancel(&self, _offer: &data_plane::blob_transfer::BlobTransferOffer) {}
}

struct StaticDiscovery(ActorAddress);

impl data_plane::namespace::NamespaceDiscovery for StaticDiscovery {
    fn current_directory(&self) -> Option<ActorAddress> {
        Some(self.0)
    }
}

struct NoopSourceRegistrar;

impl data_plane::source::BlobSourcePublisher for NoopSourceRegistrar {
    fn publish_source(&self, _source: ActorAddress) -> Result<(), String> {
        Ok(())
    }
}

struct RejectingStreamTransport;

impl data_plane::stream_transport::StreamTransport for RejectingStreamTransport {
    fn descriptor(&self) -> Result<data_plane::stream_transport::StreamPeerDescriptor, String> {
        Ok(data_plane::stream_transport::StreamPeerDescriptor(vec![1]))
    }

    fn install_source(
        &self,
        _request: data_plane::stream_transport::StreamSourceRequest,
    ) -> Result<(), String> {
        Err("injected source transport failure".to_owned())
    }

    fn install_sink(
        &self,
        _request: data_plane::stream_transport::StreamSinkRequest,
    ) -> Result<(), String> {
        Err("injected sink transport failure".to_owned())
    }

    fn source_progress(&self, _incarnation: data_plane::namespace::StreamIncarnation) {}

    fn sink_progress(&self, _incarnation: data_plane::namespace::StreamIncarnation) {}

    fn terminate(&self, _incarnation: data_plane::namespace::StreamIncarnation) {}
}


fn harness(arena_bytes: u64) -> Harness {
    harness_with_transport(
        arena_bytes,
        Arc::new(data_plane::stream_transport::LocalStreamTransport::new()),
    )
}

fn harness_with_transport(
    arena_bytes: u64,
    stream_transport: Arc<dyn data_plane::stream_transport::StreamTransport>,
) -> Harness {
    let temp = TempState::new();
    let mut arena = ArenaManager::boot(ArenaConfig {
        node_id: NodeId(1),
        reservation_ceiling: arena_bytes,
        base_alignment: 64,
    })
    .expect("host arena");
    let prepared = bootstrap::prepare_arena(
        &mut arena,
        BootstrapSpec {
            arena_generation: ARENA_GENERATION,
            alignment: 64,
        },
    )
    .expect("bootstrap");

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
        TokioBackend::new(TokioConfig {
            worker_threads: 1,
            ..TokioConfig::default()
        })
        .expect("host backend"),
    )
    .expect("host engine");
    let child_engine = Engine::new(
        child_parts,
        TokioBackend::new(TokioConfig {
            worker_threads: 1,
            ..TokioConfig::default()
        })
        .expect("child backend"),
    )
    .expect("child engine");

    let sender: Arc<dyn data_plane::blob_transfer::BlobTransferSender> = Arc::new(LoopbackSender {
        runtime: host_runtime.clone(),
    });
    let directory_actor = data_plane::namespace::DataDirectoryActor::recover(
        temp.root.join("namespace.json"),
        None,
        |_record, _length| {
            Err(data_plane::namespace::NamespaceError::SourceRecovery(
                "unexpected recovery".to_owned(),
            ))
        },
    )
    .unwrap();
    let directory = host_runtime.spawn(directory_actor).unwrap();
    let directory_client =
        data_plane::namespace::DirectoryClient::new(host_runtime.clone(), directory);
    for (logical, name, bytes) in [
        ("/models/tiny-linear/weights", "weights.bin", WEIGHTS),
        ("/models/second", "second.bin", b"second-blob".as_slice()),
    ] {
        let file_path = temp.root.join(name);
        std::fs::write(&file_path, bytes).unwrap();
        let source = data_plane::source::FileBlobSourceActor::open(
            host_runtime.clone(),
            Arc::clone(&sender),
            &file_path,
        )
        .unwrap();
        let length = source.length();
        let recovery = source.recovery();
        let source = host_runtime.spawn(source).unwrap();
        future::block_on(directory_client.register(
            path(logical),
            source,
            length,
            recovery,
            data_plane::namespace::OperationId::from_u128(u128::from(length) + 1),
        ))
        .unwrap();
    }
    let proxy = host_runtime
        .spawn(data_plane::namespace::NamespaceClientActor::new(
            host_engine.handle(),
            host_runtime.create_sender(),
            Arc::new(StaticDiscovery(directory)),
            Duration::from_millis(5),
        ))
        .unwrap();
    let namespace = data_plane::namespace::NamespaceClient::new(host_runtime.clone(), proxy);
    let host_session = host_runtime
        .spawn(
            HostDataPlaneSessionActor::new(HostDataPlaneConfig {
                runtime: host_runtime.clone(),
                engine: host_engine.handle(),
                arena,
                arena_generation: ARENA_GENERATION,
                session_generation: SESSION_GENERATION,
                capability: CAPABILITY,
                session_access: SessionAccess {
                    execution_id: "run-7".to_owned(),
                    read_prefixes: vec![path("/models"), path("/runs/run-7/results")],
                    write_prefixes: vec![path("/runs/run-7/results")],
                },
                namespace: Some(namespace),
                transfer_receiver: Some(Arc::new(DirectReceiver)),
                source_sender: Some(sender),
                source_publisher: Some(Arc::new(NoopSourceRegistrar)),
                route_registrar: None,
                stream_transport: Some(stream_transport),
            })
            .expect("host session config"),
        )
        .expect("spawn host session");
    let bootstrap = future::block_on(DataPlaneBootstrap::attach(
        prepared.arena_fd,
        child_runtime,
        host_session,
        CAPABILITY,
    ))
    .expect("routed attachment");

    Harness {
        _host_engine: host_engine,
        _child_engine: child_engine,
        _temp: temp,
        _host_runtime: host_runtime,
        bootstrap,
    }
}
