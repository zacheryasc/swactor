#![cfg(target_os = "linux")]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId};
use data_plane::blob::{
    BLOB_HEADER_LEN, Blob, BlobError, BlobLease, BlobMetadata, BlobSharedState, LeaseReleaser,
};
use data_plane::bootstrap::{self, BootstrapSpec};
use data_plane::data_plane::DataPlaneBootstrap;
use data_plane::host::{HostDataPlaneConfig, HostDataPlaneSessionActor};
use data_plane::path::{DataPath, JobContext};
use data_plane::protocol::{DataPlaneError, JobCapability};
use futures_lite::future::{self, FutureExt};
use swactor::Error;
use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::{RemoteSink, Runtime, RuntimeParts};
use swactor_engine::{Engine, TokioBackend, TokioConfig};

const CAPABILITY: JobCapability = JobCapability::new([9; 32]);
const ARENA_GENERATION: u64 = 17;
const SESSION_GENERATION: u64 = 29;
const WEIGHTS: &[u8] = b"0123456789abcdefghijklmn";

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

struct BlackHoleSink;

impl RemoteSink for BlackHoleSink {
    fn send(
        &self,
        _address: ActorAddress,
        _message: Box<dyn std::any::Any + Send>,
    ) -> Result<(), Error> {
        Ok(())
    }
}

struct Harness {
    _host_engine: Engine,
    _child_engine: Engine,
    _temp: TempState,
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

fn harness(arena_bytes: u64) -> Harness {
    let temp = TempState::new();
    let mut arena = ArenaManager::boot(ArenaConfig {
        node_id: NodeId(1),
        reservation_ceiling: arena_bytes,
        base_alignment: 64,
    })
    .expect("host arena");
    let handoff = bootstrap::write_bootstrap(
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
        TokioBackend::new(TokioConfig::default()).expect("host backend"),
    )
    .expect("host engine");
    let child_engine = Engine::new(
        child_parts,
        TokioBackend::new(TokioConfig::default()).expect("child backend"),
    )
    .expect("child engine");

    let sender: Arc<dyn data_plane::blob_transfer::BlobTransferSender> = Arc::new(LoopbackSender {
        runtime: host_runtime.clone(),
    });
    let directory_actor = data_plane::namespace::DataDirectoryActor::recover(
        temp.root.join("namespace.json"),
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
                arena,
                arena_generation: ARENA_GENERATION,
                session_generation: SESSION_GENERATION,
                capability: CAPABILITY,
                job_context: JobContext {
                    run_id: "run-7".to_owned(),
                    read_prefixes: vec![path("/models"), path("/runs/run-7/results")],
                    write_prefixes: vec![path("/runs/run-7/results")],
                },
                namespace: Some(namespace),
                transfer_receiver: Some(Arc::new(DirectReceiver)),
                source_sender: Some(sender),
                source_publisher: Some(Arc::new(NoopSourceRegistrar)),
                route_registrar: None,
            })
            .expect("host session config"),
        )
        .expect("spawn host session");
    let bootstrap = future::block_on(DataPlaneBootstrap::attach(
        handoff.arena_fd,
        child_runtime,
        host_session,
        CAPABILITY,
    ))
    .expect("routed attachment");

    Harness {
        _host_engine: host_engine,
        _child_engine: child_engine,
        _temp: temp,
        bootstrap,
    }
}

#[derive(Default)]
struct NoopReleaser;

impl LeaseReleaser for NoopReleaser {
    fn release(&self, _lease: BlobLease) {}
}

#[test]
fn attachment_without_a_host_reply_fails_on_actor_deadline() {
    let mut arena = ArenaManager::boot(ArenaConfig {
        node_id: NodeId(8),
        reservation_ceiling: 4096,
        base_alignment: 64,
    })
    .unwrap();
    let handoff = bootstrap::write_bootstrap(
        &mut arena,
        BootstrapSpec {
            arena_generation: ARENA_GENERATION,
            alignment: 64,
        },
    )
    .unwrap();
    let (parts, runtime) = runtime_parts();
    runtime.set_remote_sink(Arc::new(BlackHoleSink));
    let engine = Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
    let (mapped, resolved) = DataPlaneBootstrap::map_arena(handoff.arena_fd).unwrap();
    let result = future::block_on(DataPlaneBootstrap::attach_mapped_with_deadline(
        mapped,
        resolved,
        runtime.clone(),
        ActorAddress::new_random(),
        CAPABILITY,
        None,
        data_plane::data_plane::AttachDeadline {
            engine: engine.handle(),
            timeout: Duration::from_millis(20),
        },
    ));
    assert!(matches!(
        result,
        Err(DataPlaneError::SessionFailed(reason)) if reason.contains("deadline")
    ));
}

#[test]
fn routed_read_blob_maps_final_sealed_lease_without_copying() {
    let harness = harness(4096);
    let blob = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/tiny-linear/weights"),
    )
    .expect("read blob");

    assert_eq!(blob.length(), 24);
    assert!(blob.digest().is_none());
    let lease = blob.lease();
    let view = blob.map().expect("map sealed blob");
    assert_eq!(view.as_ref(), WEIGHTS);
    assert_eq!(
        view.as_ptr(),
        // SAFETY: the lease is validated and the expected payload offset lies
        // inside the mapping retained by the test harness.
        unsafe {
            harness
                .bootstrap
                .arena
                .base_ptr()
                .add((lease.offset + BLOB_HEADER_LEN) as usize)
        }
    );

    let mut stale = lease;
    stale.generation += 1;
    let error = Blob::from_sealed_lease(
        harness.bootstrap.arena.clone(),
        stale,
        BlobMetadata {
            length: blob.length(),
            digest: blob.digest().copied(),
        },
        Arc::new(NoopReleaser),
    )
    .expect_err("stale generation must fail");
    assert!(matches!(error, BlobError::StaleGeneration { .. }));

    let mut outside = lease;
    outside.offset = u64::MAX - 32;
    let error = Blob::from_sealed_lease(
        harness.bootstrap.arena.clone(),
        outside,
        BlobMetadata {
            length: blob.length(),
            digest: blob.digest().copied(),
        },
        Arc::new(NoopReleaser),
    )
    .expect_err("out-of-bounds descriptor must fail");
    assert!(matches!(error, BlobError::RangeOutOfBounds { .. }));

    // Simulate a descriptor granted before its producer release-publishes the
    // sealed state. Validation must acquire and reject it before exposing bytes.
    let state_offset = lease.offset as usize + 24;
    // SAFETY: blob headers are 64-byte aligned and the state field is an
    // aligned AtomicU64 at the stable ABI offset 24.
    let state = unsafe {
        &*harness
            .bootstrap
            .arena
            .base_ptr()
            .add(state_offset)
            .cast::<AtomicU64>()
    };
    state.store(BlobSharedState::Filling as u64, Ordering::Release);
    let error = Blob::from_sealed_lease(
        harness.bootstrap.arena.clone(),
        lease,
        BlobMetadata {
            length: blob.length(),
            digest: blob.digest().copied(),
        },
        Arc::new(NoopReleaser),
    )
    .expect_err("early grant must fail");
    assert!(matches!(error, BlobError::InvalidState { .. }));
}

#[test]
fn thirty_two_concurrent_remote_opens_complete_without_cross_wiring() {
    fn reads(
        data_plane: data_plane::data_plane::DataPlane,
        first: usize,
        count: usize,
    ) -> future::Boxed<Vec<(usize, Blob)>> {
        if count == 1 {
            return async move {
                let path = if first.is_multiple_of(2) {
                    "/models/tiny-linear/weights"
                } else {
                    "/models/second"
                };
                vec![(
                    first,
                    data_plane
                        .read_blob_path(path)
                        .await
                        .expect("concurrent open"),
                )]
            }
            .boxed();
        }
        let left_count = count / 2;
        let left = reads(data_plane.clone(), first, left_count);
        let right = reads(data_plane, first + left_count, count - left_count);
        async move {
            let (mut left, right) = future::zip(left, right).await;
            left.extend(right);
            left
        }
        .boxed()
    }

    let harness = harness(16 * 1024);
    let blobs = future::block_on(reads(harness.bootstrap.data_plane.clone(), 0, 32));
    for (index, blob) in blobs {
        let expected = if index % 2 == 0 {
            WEIGHTS
        } else {
            b"second-blob"
        };
        assert_eq!(blob.map().unwrap().as_ref(), expected);
    }
}

#[test]
fn concurrent_remote_opens_keep_actor_identity_correlation() {
    let harness = harness(4096);
    let (weights, second) = future::block_on(future::zip(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/tiny-linear/weights"),
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/second"),
    ));
    assert_eq!(weights.unwrap().map().unwrap().as_ref(), WEIGHTS);
    assert_eq!(second.unwrap().map().unwrap().as_ref(), b"second-blob");
}

#[test]
fn live_view_prevents_reclaim_until_last_guard_drops() {
    let harness = harness(256);
    let blob = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/tiny-linear/weights"),
    )
    .expect("first read");
    let view = blob.map().expect("view");
    drop(blob);

    let blocked = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/tiny-linear/weights"),
    );
    assert!(matches!(blocked, Err(DataPlaneError::ArenaExhausted)));

    drop(view);
    std::thread::sleep(Duration::from_millis(10));
    let reopened = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/tiny-linear/weights"),
    )
    .expect("lease is reclaimable after the final view drops");
    assert_eq!(reopened.map().unwrap().as_ref(), WEIGHTS);
}

#[test]
fn path_absence_and_authorization_fail_before_blob_success() {
    let harness = harness(4096);
    let missing = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/models/missing"),
    );
    assert!(matches!(missing, Err(DataPlaneError::PathNotFound(_))));

    let unauthorized = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/runs/self/private"),
    );
    assert!(matches!(
        unauthorized,
        Err(DataPlaneError::Unauthorized { .. })
    ));
}

#[test]
fn cancelled_write_open_releases_queued_grant() {
    let harness = harness(256);
    let mut cancelled = Box::pin(
        harness
            .bootstrap
            .data_plane
            .write_blob_path("/runs/self/results/cancelled", 8),
    );
    assert!(
        future::block_on(future::poll_once(cancelled.as_mut())).is_none(),
        "first poll only submits the actor operation"
    );
    std::thread::sleep(Duration::from_millis(10));
    drop(cancelled);
    std::thread::sleep(Duration::from_millis(10));

    let mut retry = future::block_on(
        harness
            .bootstrap
            .data_plane
            .write_blob_path("/runs/self/results/cancelled", 8),
    )
    .expect("cancelled grant was reclaimed");
    future::block_on(retry.abort()).unwrap();
}

#[test]
fn write_blob_seals_once_and_abort_publishes_nothing() {
    let harness = harness(4096);
    let mut writer = future::block_on(
        harness
            .bootstrap
            .data_plane
            .write_blob_path("/runs/self/results/blob", 6),
    )
    .expect("write grant");
    let mut view = writer.map().expect("writable view");
    view.copy_from_slice(b"result");
    assert!(matches!(
        future::block_on(writer.seal()),
        Err(DataPlaneError::Blob(
            data_plane::protocol::BlobFailure::ActiveWritableView
        ))
    ));
    drop(view);
    future::block_on(writer.seal()).expect("seal and publish");

    let published = future::block_on(
        harness
            .bootstrap
            .data_plane
            .read_blob_path("/runs/self/results/blob"),
    )
    .expect("clean exit publishes exactly once");
    assert_eq!(published.length(), 6);
    assert_eq!(published.map().unwrap().as_ref(), b"result");
    assert!(future::block_on(writer.seal()).is_err());

    let mut aborted = future::block_on(
        harness
            .bootstrap
            .data_plane
            .write_blob_path("/runs/self/results/aborted", 4),
    )
    .expect("abort grant");
    let active = aborted.map().expect("active writable view");
    assert!(future::block_on(aborted.abort()).is_err());
    drop(active);
    future::block_on(aborted.abort()).expect("abort after view closes");
    assert!(matches!(
        future::block_on(
            harness
                .bootstrap
                .data_plane
                .read_blob_path("/runs/self/results/aborted")
        ),
        Err(DataPlaneError::PathNotFound(_))
    ));
}
