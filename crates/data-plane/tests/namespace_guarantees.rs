use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use data_plane::host::{HostRouteRegistrar, HostRouteWatch};
use data_plane::namespace::{
    DataDirectoryActor, DataDirectoryIn, DataDirectoryOut, DirectoryClient, DirectoryRequestId,
    EntryKind, NamespaceClient, NamespaceClientActor, NamespaceClientIn, NamespaceDiscovery,
    NamespaceError, NamespaceRequest, OperationId, RetirementRetry, SourceRecovery, StreamRole,
};
use data_plane::path::DataPath;
use futures_lite::future;
use parking_lot::RwLock;
use proptest::prelude::*;
use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::{Runtime, RuntimeParts};
use swactor_engine::{Engine, SteppingBackend, TokioBackend, TokioConfig};

static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

struct TempState {
    root: PathBuf,
}

impl TempState {
    fn new(label: &str) -> Self {
        let sequence = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "swactor-namespace-{label}-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("create temporary namespace directory");
        Self { root }
    }

    fn store(&self) -> PathBuf {
        self.root.join("namespace.json")
    }
}

impl Drop for TempState {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

struct DirectoryHarness {
    engine: Engine,
    runtime: Runtime,
    client: DirectoryClient,
}

fn path(value: &str) -> DataPath {
    DataPath::parse(value).expect("test path")
}

fn source(byte: u8) -> ActorAddress {
    ActorAddress([byte; 32])
}

fn recovery(actor: ActorAddress) -> SourceRecovery {
    SourceRecovery::Actor {
        actor,
        node: [9; 32],
        owner: None,
    }
}
fn spawn_directory(store: &Path) -> DirectoryHarness {
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig {
            worker_threads: 1,
            ..TokioConfig::default()
        })
        .expect("tokio backend"),
    )
    .expect("directory engine");
    let actor = DataDirectoryActor::recover(
        store,
        Some(RetirementRetry::new(
            engine.handle(),
            runtime.create_sender(),
            Duration::from_millis(50),
            None,
        )),
        |record, _length| match record {
            SourceRecovery::Actor { actor, node, .. } => Ok((*actor, *node)),
            SourceRecovery::File { path } => Err(NamespaceError::SourceRecovery(format!(
                "test cannot recover file source {}",
                path.display()
            ))),
        },
    )
    .expect("recover directory");
    let directory = runtime.spawn(actor).expect("spawn directory actor");
    DirectoryHarness {
        engine,
        runtime: runtime.clone(),
        client: DirectoryClient::new(runtime, directory),
    }
}

#[test]
fn namespace_mutations_are_linearizable_and_durable() {
    let state = TempState::new("linearizable");
    let directory = spawn_directory(&state.store());
    let logical = path("/models/tiny-linear/weights");
    let first = source(1);
    let second = source(2);

    future::block_on(async {
        let registered = directory
            .client
            .register(
                logical.clone(),
                first,
                [9; 32],
                24,
                recovery(first),
                OperationId::from_u128(1),
            )
            .await
            .expect("register first source");
        assert_eq!(registered.revision, 1);
        let node = directory
            .client
            .lookup(logical.clone())
            .await
            .expect("lookup first blob node");
        assert_eq!(node.kind, EntryKind::Blob);
        assert_eq!(node.revision, registered.revision);

        let selected_first = directory
            .client
            .resolve(logical.clone())
            .await
            .expect("resolve first source");
        assert_eq!(selected_first.source, first);
        assert_eq!(selected_first.length, 24);
        assert_eq!(selected_first.revision, 1);

        let replaced = directory
            .client
            .register(
                logical.clone(),
                second,
                [9; 32],
                32,
                recovery(second),
                OperationId::from_u128(2),
            )
            .await
            .expect("replace source");
        assert_eq!(replaced.revision, 2);

        let selected_second = directory
            .client
            .resolve(logical.clone())
            .await
            .expect("resolve replacement");
        assert_eq!(selected_second.source, second);
        assert_eq!(selected_second.length, 32);
        assert_eq!(selected_second.revision, 2);

        // A completed resolve is a binding snapshot. Later replacement cannot
        // mutate the source selected by the earlier logical read.
        assert_eq!(selected_first.source, first);
        assert_eq!(selected_first.revision, 1);

        let removed = directory
            .client
            .unregister(logical.clone(), OperationId::from_u128(3))
            .await
            .expect("unregister source");
        assert_eq!(removed.revision, 3);
        assert!(matches!(
            directory.client.resolve(logical.clone()).await,
            Err(NamespaceError::PathNotFound(found)) if found == logical
        ));
    });

    // The acknowledged removal is recovered from a fresh actor instance.
    let recovered = spawn_directory(&state.store());
    assert!(matches!(
        future::block_on(recovered.client.resolve(logical.clone())),
        Err(NamespaceError::PathNotFound(found)) if found == logical
    ));
}

#[test]
fn rename_is_atomic_replayable_and_revisioned() {
    let state = TempState::new("rename");
    let directory = spawn_directory(&state.store());
    let source_path = path("/rename/source");
    let destination_path = path("/rename/destination");
    let source_actor = source(7);
    let destination_actor = source(8);
    let owner = source(9);

    future::block_on(async {
        directory
            .client
            .register(
                source_path.clone(),
                source_actor,
                [9; 32],
                7,
                SourceRecovery::Actor {
                    actor: source_actor,
                    node: [9; 32],
                    owner: Some(owner),
                },
                OperationId::from_u128(100),
            )
            .await
            .unwrap();
        directory
            .client
            .register(
                destination_path.clone(),
                destination_actor,
                [9; 32],
                8,
                recovery(destination_actor),
                OperationId::from_u128(101),
            )
            .await
            .unwrap();

        assert!(matches!(
            directory
                .client
                .rename(
                    source_path.clone(),
                    destination_path.clone(),
                    false,
                    OperationId::from_u128(102),
                )
                .await,
            Err(NamespaceError::PathExists(path)) if path == destination_path
        ));
        assert_eq!(
            directory
                .client
                .resolve(source_path.clone())
                .await
                .unwrap()
                .source,
            source_actor
        );
        assert_eq!(
            directory
                .client
                .resolve(destination_path.clone())
                .await
                .unwrap()
                .source,
            destination_actor
        );

        let renamed = directory
            .client
            .rename(
                source_path.clone(),
                destination_path.clone(),
                true,
                OperationId::from_u128(103),
            )
            .await
            .unwrap();
        assert_eq!(renamed.revision, 3);
        let replayed = directory
            .client
            .rename(
                source_path.clone(),
                destination_path.clone(),
                true,
                OperationId::from_u128(103),
            )
            .await
            .unwrap();
        assert_eq!(replayed, renamed);
        assert!(matches!(
            directory.client.lookup(source_path.clone()).await,
            Err(NamespaceError::PathNotFound(path)) if path == source_path
        ));
        let resolved = directory
            .client
            .resolve(destination_path.clone())
            .await
            .unwrap();
        assert_eq!(resolved.source, source_actor);
        assert_eq!(resolved.revision, renamed.revision);
        assert_eq!(resolved.owner, Some(owner));
    });

    let recovered = spawn_directory(&state.store());
    let resolved = future::block_on(recovered.client.resolve(destination_path)).unwrap();
    assert_eq!(resolved.source, source_actor);
    assert_eq!(resolved.revision, 3);
    assert_eq!(resolved.owner, Some(owner));
}

#[test]
fn mutation_rejections_are_sticky_across_reservation_lifecycle() {
    let state = TempState::new("sticky-rejections");
    let directory = spawn_directory(&state.store());
    let source_path = path("/sticky/source");
    let reserved_path = path("/sticky/reserved");
    let unbound_path = path("/sticky/unbound");
    let stream_path = path("/sticky/stream");
    let source_actor = source(9);

    future::block_on(async {
        directory
            .client
            .register(
                source_path.clone(),
                source_actor,
                [9; 32],
                5,
                recovery(source_actor),
                OperationId::from_u128(200),
            )
            .await
            .unwrap();

        // A concurrent blob upload holds a reservation on the rename target.
        directory
            .client
            .reserve_blob(reserved_path.clone(), OperationId::from_u128(201))
            .await
            .unwrap();
        assert!(matches!(
            directory
                .client
                .rename(
                    source_path.clone(),
                    reserved_path.clone(),
                    true,
                    OperationId::from_u128(202),
                )
                .await,
            Err(NamespaceError::PathExists(path)) if path == reserved_path
        ));
        // The upload is abandoned and its reservation released. Retrying the
        // SAME operation id must replay the rejection: an id the client saw
        // rejected may never commit later, whatever else changed.
        directory
            .client
            .release_blob_reservation(reserved_path.clone(), OperationId::from_u128(201))
            .await
            .unwrap();
        assert!(matches!(
            directory
                .client
                .rename(
                    source_path.clone(),
                    reserved_path.clone(),
                    true,
                    OperationId::from_u128(202),
                )
                .await,
            Err(NamespaceError::PathExists(path)) if path == reserved_path
        ));
        assert_eq!(
            directory
                .client
                .resolve(source_path.clone())
                .await
                .unwrap()
                .source,
            source_actor
        );

        // Unregister against a reserved-but-unbound path is sticky the same way.
        directory
            .client
            .reserve_blob(unbound_path.clone(), OperationId::from_u128(203))
            .await
            .unwrap();
        assert!(matches!(
            directory
                .client
                .unregister(unbound_path.clone(), OperationId::from_u128(204))
                .await,
            Err(NamespaceError::PathExists(path)) if path == unbound_path
        ));
        directory
            .client
            .release_blob_reservation(unbound_path.clone(), OperationId::from_u128(203))
            .await
            .unwrap();
        assert!(matches!(
            directory
                .client
                .unregister(unbound_path.clone(), OperationId::from_u128(204))
                .await,
            Err(NamespaceError::PathExists(path)) if path == unbound_path
        ));

        // Stream binding against a reserved path replays its rejection too.
        directory
            .client
            .reserve_blob(stream_path.clone(), OperationId::from_u128(205))
            .await
            .unwrap();
        assert!(matches!(
            directory
                .client
                .replace_with_stream(
                    stream_path.clone(),
                    StreamRole::Source,
                    source(4),
                    OperationId::from_u128(206),
                )
                .await,
            Err(NamespaceError::PathExists(path)) if path == stream_path
        ));
        directory
            .client
            .release_blob_reservation(stream_path.clone(), OperationId::from_u128(205))
            .await
            .unwrap();
        assert!(matches!(
            directory
                .client
                .replace_with_stream(
                    stream_path.clone(),
                    StreamRole::Source,
                    source(4),
                    OperationId::from_u128(206),
                )
                .await,
            Err(NamespaceError::PathExists(path)) if path == stream_path
        ));
    });
}

#[test]
fn committed_mutations_replay_even_when_path_is_reserved_again() {
    let state = TempState::new("replay-across-reservation");
    let directory = spawn_directory(&state.store());
    let logical = path("/replay/reserved");
    let blob = source(3);

    future::block_on(async {
        directory
            .client
            .register(
                logical.clone(),
                blob,
                [9; 32],
                12,
                recovery(blob),
                OperationId::from_u128(300),
            )
            .await
            .unwrap();
        let removed = directory
            .client
            .unregister(logical.clone(), OperationId::from_u128(301))
            .await
            .unwrap();

        // A later upload reserves the freed path. A duplicate delivery of the
        // already-acknowledged unregister must still replay its receipt
        // instead of failing against the unrelated reservation.
        directory
            .client
            .reserve_blob(logical.clone(), OperationId::from_u128(302))
            .await
            .unwrap();
        let replayed = directory
            .client
            .unregister(logical.clone(), OperationId::from_u128(301))
            .await
            .unwrap();
        assert_eq!(replayed, removed);
    });
}

#[test]
fn active_stream_rename_is_typed_and_quiescent_stream_rename_succeeds() {
    let state = TempState::new("stream-rename");
    let directory = spawn_directory(&state.store());
    let source_path = path("/rename/stream-source");
    let destination_path = path("/rename/stream-destination");

    future::block_on(async {
        let mut source_open = Box::pin(directory.client.open_stream(
            source_path.clone(),
            StreamRole::Source,
            source(30),
            OperationId::from_u128(110),
        ));
        assert!(future::poll_once(source_open.as_mut()).await.is_none());
        let sink_match = directory
            .client
            .open_stream(
                source_path.clone(),
                StreamRole::Sink,
                source(31),
                OperationId::from_u128(111),
            )
            .await
            .unwrap();
        let source_match = source_open.await.unwrap();
        assert_eq!(source_match, sink_match);
        let missing_source = path("/rename/missing-source");
        assert!(matches!(
            directory
                .client
                .rename(
                    missing_source.clone(),
                    source_path.clone(),
                    true,
                    OperationId::from_u128(114),
                )
                .await,
            Err(NamespaceError::PathNotFound(path)) if path == missing_source
        ));

        assert!(matches!(
            directory
                .client
                .rename(
                    source_path.clone(),
                    destination_path.clone(),
                    false,
                    OperationId::from_u128(112),
                )
                .await,
            Err(NamespaceError::StreamActive(path)) if path == source_path
        ));
        directory
            .client
            .close_stream(source_path.clone(), source_match.incarnation)
            .await
            .unwrap();
        let renamed = directory
            .client
            .rename(
                source_path.clone(),
                destination_path.clone(),
                false,
                OperationId::from_u128(113),
            )
            .await
            .unwrap();
        let node = directory
            .client
            .lookup(destination_path.clone())
            .await
            .unwrap();
        assert_eq!(node.kind, EntryKind::Stream);
        assert_eq!(node.revision, renamed.revision);
        assert!(matches!(
            directory.client.lookup(source_path.clone()).await,
            Err(NamespaceError::PathNotFound(path)) if path == source_path
        ));
    });
}

#[test]
fn stream_rendezvous_is_symmetric_and_incarnations_are_isolated() {
    let state = TempState::new("stream-rendezvous");
    let directory = spawn_directory(&state.store());
    let logical = path("/runs/7/results");

    future::block_on(async {
        let mut source_open = Box::pin(directory.client.open_stream(
            logical.clone(),
            StreamRole::Source,
            source(10),
            OperationId::from_u128(10),
        ));
        assert!(future::poll_once(source_open.as_mut()).await.is_none());
        let node = directory
            .client
            .lookup(logical.clone())
            .await
            .expect("lookup ensured stream node");
        assert_eq!(node.kind, EntryKind::Stream);

        let sink_match = directory
            .client
            .open_stream(
                logical.clone(),
                StreamRole::Sink,
                source(11),
                OperationId::from_u128(11),
            )
            .await
            .expect("sink matches source");
        let source_match = source_open.await.expect("source matches sink");
        assert_eq!(source_match, sink_match);
        assert_eq!(source_match.source, source(10));
        assert_eq!(source_match.sink, source(11));

        directory
            .client
            .close_stream(logical.clone(), source_match.incarnation)
            .await
            .expect("close first incarnation");
        directory
            .client
            .close_stream(logical.clone(), source_match.incarnation)
            .await
            .expect("duplicate close is idempotent");

        assert!(matches!(
            directory
                .client
                .open_stream(
                    logical.clone(),
                    StreamRole::Source,
                    source(10),
                    OperationId::from_u128(10),
                )
                .await,
            Err(NamespaceError::StaleIncarnation { .. })
        ));
        assert!(matches!(
            directory
                .client
                .open_stream(
                    logical.clone(),
                    StreamRole::Sink,
                    source(11),
                    OperationId::from_u128(11),
                )
                .await,
            Err(NamespaceError::StaleIncarnation { .. })
        ));

        let mut sink_open = Box::pin(directory.client.open_stream(
            logical.clone(),
            StreamRole::Sink,
            source(12),
            OperationId::from_u128(12),
        ));
        assert!(future::poll_once(sink_open.as_mut()).await.is_none());
        assert!(matches!(
            directory
                .client
                .open_stream(
                    logical.clone(),
                    StreamRole::Source,
                    source(10),
                    OperationId::from_u128(10),
                )
                .await,
            Err(NamespaceError::StaleIncarnation { .. })
        ));
        let second_source = directory
            .client
            .open_stream(
                logical,
                StreamRole::Source,
                source(13),
                OperationId::from_u128(13),
            )
            .await
            .expect("source matches waiting sink");
        let second_sink = sink_open.await.expect("sink matches source");
        assert_eq!(second_source, second_sink);
        assert_ne!(source_match.incarnation, second_source.incarnation);
    });
}

#[test]
fn typed_paths_require_explicit_rebinding() {
    let state = TempState::new("typed-path");
    let directory = spawn_directory(&state.store());
    let logical = path("/typed/value");
    let blob_source = source(20);

    future::block_on(async {
        directory
            .client
            .register(
                logical.clone(),
                blob_source,
                [9; 32],
                4,
                recovery(blob_source),
                OperationId::from_u128(20),
            )
            .await
            .expect("register blob");

        assert!(matches!(
            directory
                .client
                .open_stream(
                    logical.clone(),
                    StreamRole::Source,
                    source(21),
                    OperationId::from_u128(21),
                )
                .await,
            Err(NamespaceError::WrongEntryType {
                expected: EntryKind::Stream,
                found: EntryKind::Blob,
                ..
            })
        ));

        let mut source_open = Box::pin(directory.client.replace_with_stream(
            logical.clone(),
            StreamRole::Source,
            source(22),
            OperationId::from_u128(22),
        ));
        assert!(future::poll_once(source_open.as_mut()).await.is_none());
        assert!(matches!(
            directory.client.resolve(logical.clone()).await,
            Err(NamespaceError::WrongEntryType {
                expected: EntryKind::Blob,
                found: EntryKind::Stream,
                ..
            })
        ));
        let sink_match = directory
            .client
            .open_stream(
                logical,
                StreamRole::Sink,
                source(23),
                OperationId::from_u128(23),
            )
            .await
            .expect("match rebound stream");
        assert_eq!(
            source_open.await.expect("rebound source matched"),
            sink_match
        );
    });
}

#[test]
fn replacing_waiting_stream_displaces_old_open() {
    let state = TempState::new("stream-displacement");
    let directory = spawn_directory(&state.store());
    let logical = path("/replace/waiting");

    future::block_on(async {
        let mut old_open = Box::pin(directory.client.open_stream(
            logical.clone(),
            StreamRole::Source,
            source(30),
            OperationId::from_u128(30),
        ));
        assert!(future::poll_once(old_open.as_mut()).await.is_none());

        let mut replacement = Box::pin(directory.client.replace_with_stream(
            logical.clone(),
            StreamRole::Source,
            source(31),
            OperationId::from_u128(31),
        ));
        assert!(future::poll_once(replacement.as_mut()).await.is_none());
        assert!(matches!(
            old_open.await,
            Err(NamespaceError::PathReplaced(found)) if found == logical
        ));

        let sink_match = directory
            .client
            .open_stream(
                logical,
                StreamRole::Sink,
                source(32),
                OperationId::from_u128(32),
            )
            .await
            .expect("sink matches replacement");
        assert_eq!(
            replacement.await.expect("replacement source matched"),
            sink_match
        );
        assert_eq!(sink_match.source, source(31));
    });
}

#[test]
fn duplicate_stream_role_fails_without_replacing_the_waiter() {
    let state = TempState::new("duplicate-stream-role");
    let directory = spawn_directory(&state.store());
    let logical = path("/duplicate/source");

    future::block_on(async {
        let mut first = Box::pin(directory.client.open_stream(
            logical.clone(),
            StreamRole::Source,
            source(51),
            OperationId::from_u128(51),
        ));
        assert!(future::poll_once(first.as_mut()).await.is_none());
        assert!(matches!(
            directory
                .client
                .open_stream(
                    logical.clone(),
                    StreamRole::Source,
                    source(52),
                    OperationId::from_u128(52),
                )
                .await,
            Err(NamespaceError::DuplicateStreamRole {
                role: StreamRole::Source,
                ..
            })
        ));
        let matched = directory
            .client
            .open_stream(
                logical,
                StreamRole::Sink,
                source(53),
                OperationId::from_u128(53),
            )
            .await
            .expect("sink matches original source");
        assert_eq!(matched.source, source(51));
        assert_eq!(first.await.expect("original source survives"), matched);
    });
}
#[test]
fn committed_mutation_retry_has_at_most_once_effect() {
    let state = TempState::new("idempotent");
    let logical = path("/models/a");
    let actor = source(7);
    let operation = OperationId::from_u128(99);

    let first_runtime = spawn_directory(&state.store());
    let first_receipt = future::block_on(first_runtime.client.register(
        logical.clone(),
        actor,
        [9; 32],
        8,
        recovery(actor),
        operation,
    ))
    .expect("initial registration");
    assert_eq!(first_receipt.revision, 1);
    drop(first_runtime);

    let recovered = spawn_directory(&state.store());
    let retried = future::block_on(recovered.client.register(
        logical.clone(),
        actor,
        [9; 32],
        8,
        recovery(actor),
        operation,
    ))
    .expect("retry committed registration");
    assert_eq!(retried, first_receipt);
    assert_eq!(
        future::block_on(recovered.client.resolve(logical.clone()))
            .expect("resolve recovered binding")
            .revision,
        1
    );

    let conflict = future::block_on(recovered.client.register(
        logical,
        source(8),
        [9; 32],
        9,
        recovery(source(8)),
        operation,
    ));
    assert!(matches!(conflict, Err(NamespaceError::OperationConflict(id)) if id == operation));
}

struct StaticDiscovery {
    directory: Arc<RwLock<Option<ActorAddress>>>,
}

impl NamespaceDiscovery for StaticDiscovery {
    fn current_directory(&self) -> Option<ActorAddress> {
        *self.directory.read()
    }
}

#[test]
fn cancelled_stream_open_never_resurrects_across_reordering() {
    let state = TempState::new("cancel-tombstone");
    let harness = spawn_directory(&state.store());
    let directory = harness.client.directory();
    let logical = path("/runs/9/parked-sink");
    let endpoint = source(21);
    let operation = OperationId::from_u128(21);
    let replies = harness
        .runtime
        .new_inbox::<NamespaceClientIn>()
        .expect("reply inbox");

    // The cancel races ahead of the open it retracts (the open frame was
    // lost): the persisted rejection must turn the open's late arrival into
    // a typed failure, never a fresh pending endpoint.
    harness
        .runtime
        .send_to(
            directory,
            DataDirectoryIn::CancelStream {
                request_id: DirectoryRequestId(1),
                path: logical.clone(),
                operation_id: operation,
                reply_to: Some(*replies.addr()),
            },
        )
        .expect("send cancel before open");
    match future::block_on(replies.recv()) {
        NamespaceClientIn::DirectoryReply(DataDirectoryOut::StreamCancelled {
            result: Ok(()),
            ..
        }) => {}
        other => panic!("expected cancel acknowledgement, got {other:?}"),
    }
    harness
        .runtime
        .send_to(
            directory,
            DataDirectoryIn::OpenStream {
                request_id: DirectoryRequestId(2),
                path: logical.clone(),
                role: StreamRole::Sink,
                descriptor: Vec::new(),
                endpoint,
                replace: false,
                ensure: true,
                expected_revision: None,
                operation_id: operation,
                reply_to: *replies.addr(),
            },
        )
        .expect("send late open replay");
    match future::block_on(replies.recv()) {
        NamespaceClientIn::DirectoryReply(DataDirectoryOut::StreamOpened {
            result: Err(NamespaceError::PathReplaced(_)),
            ..
        }) => {}
        other => panic!("tombstoned open must replay its rejection, got {other:?}"),
    }
    assert!(
        matches!(
            future::block_on(harness.client.lookup(logical.clone())),
            Err(NamespaceError::PathNotFound(_))
        ),
        "a cancelled open must not leave a stream node behind"
    );

    // Once the open did land, cancelling it and replaying it again must be
    // equally terminal: no resurrected waiter holds the path hostage.
    let second = OperationId::from_u128(22);
    harness
        .runtime
        .send_to(
            directory,
            DataDirectoryIn::OpenStream {
                request_id: DirectoryRequestId(3),
                path: logical.clone(),
                role: StreamRole::Sink,
                descriptor: Vec::new(),
                endpoint,
                replace: false,
                ensure: true,
                expected_revision: None,
                operation_id: second,
                reply_to: *replies.addr(),
            },
        )
        .expect("send fresh open");
    let node = future::block_on(harness.client.lookup(logical.clone()))
        .expect("waiting sink publishes its stream node");
    assert!(node.active, "waiting sink keeps the node active");
    harness
        .runtime
        .send_to(
            directory,
            DataDirectoryIn::CancelStream {
                request_id: DirectoryRequestId(4),
                path: logical.clone(),
                operation_id: second,
                reply_to: Some(*replies.addr()),
            },
        )
        .expect("cancel the waiting open");
    match future::block_on(replies.recv()) {
        NamespaceClientIn::DirectoryReply(DataDirectoryOut::StreamCancelled {
            result: Ok(()),
            ..
        }) => {}
        other => panic!("expected second cancel acknowledgement, got {other:?}"),
    }
    harness
        .runtime
        .send_to(
            directory,
            DataDirectoryIn::OpenStream {
                request_id: DirectoryRequestId(5),
                path: logical.clone(),
                role: StreamRole::Sink,
                descriptor: Vec::new(),
                endpoint,
                replace: false,
                ensure: true,
                expected_revision: None,
                operation_id: second,
                reply_to: *replies.addr(),
            },
        )
        .expect("replay the cancelled open");
    match future::block_on(replies.recv()) {
        NamespaceClientIn::DirectoryReply(DataDirectoryOut::StreamOpened {
            result: Err(NamespaceError::StaleIncarnation { .. }),
            ..
        }) => {}
        other => panic!("replayed cancelled open must be stale, got {other:?}"),
    }
    let node = future::block_on(harness.client.lookup(logical.clone()))
        .expect("stream node survives until unlink");
    assert!(!node.active, "cancelled sink must release the path");
    future::block_on(
        harness
            .client
            .unregister(logical.clone(), OperationId::from_u128(23)),
    )
    .expect("quiescent stream node unlinks");
}

struct ForwardingSink {
    destination: Runtime,
}

impl swactor::runtime::RemoteSink for ForwardingSink {
    fn send(
        &self,
        address: ActorAddress,
        message: Box<dyn std::any::Any + Send>,
    ) -> Result<(), swactor::Error> {
        self.destination.deliver_raw(address, message)
    }
}

struct CancelDroppingSink {
    destination: Runtime,
    drop_next_cancel: AtomicBool,
}

impl swactor::runtime::RemoteSink for CancelDroppingSink {
    fn send(
        &self,
        address: ActorAddress,
        message: Box<dyn std::any::Any + Send>,
    ) -> Result<(), swactor::Error> {
        if let Some(frame) = message.downcast_ref::<DataDirectoryIn>()
            && matches!(frame, DataDirectoryIn::CancelStream { .. })
            && self.drop_next_cancel.swap(false, Ordering::SeqCst)
        {
            // Simulate one lost frame on the at-most-once path.
            return Ok(());
        }
        self.destination.deliver_raw(address, message)
    }
}

#[test]
fn cancelled_stream_open_retracts_despite_a_lost_cancel_frame() {
    let state = TempState::new("cancel-retry");
    let directory_harness = spawn_directory(&state.store());
    let directory = directory_harness.client.directory();

    let client_parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let client_runtime = client_parts.runtime().clone();
    let client_engine = Engine::new(
        client_parts,
        TokioBackend::new(TokioConfig {
            worker_threads: 1,
            ..TokioConfig::default()
        })
        .expect("tokio backend"),
    )
    .expect("client engine");
    client_runtime.set_remote_sink(Arc::new(CancelDroppingSink {
        destination: directory_harness.runtime.clone(),
        drop_next_cancel: AtomicBool::new(true),
    }));
    directory_harness
        .runtime
        .set_remote_sink(Arc::new(ForwardingSink {
            destination: client_runtime.clone(),
        }));

    let discovered = Arc::new(RwLock::new(Some(directory)));
    let proxy = client_runtime
        .spawn(NamespaceClientActor::new(
            client_engine.handle(),
            client_runtime.create_sender(),
            Arc::new(StaticDiscovery {
                directory: Arc::clone(&discovered),
            }),
            Duration::from_millis(20),
        ))
        .expect("spawn namespace proxy");
    let open_replies = client_runtime
        .new_inbox::<NamespaceClientIn>()
        .expect("open reply inbox");

    client_runtime
        .send_to(
            proxy,
            NamespaceClientIn::Request {
                request: NamespaceRequest::OpenStream {
                    path: path("/runs/11/parked"),
                    role: StreamRole::Sink,
                    endpoint: source(31),
                    descriptor: Vec::new(),
                    replace: false,
                    ensure: true,
                    expected_revision: None,
                    operation_id: OperationId::from_u128(31),
                },
                reply_to: *open_replies.addr(),
            },
        )
        .expect("send parked sink open");
    let bound = Instant::now() + Duration::from_secs(10);
    loop {
        let active = matches!(
            future::block_on(directory_harness.client.lookup(path("/runs/11/parked"))),
            Ok(node) if node.active
        );
        if active {
            break;
        }
        assert!(
            Instant::now() < bound,
            "parked sink open never reached the directory"
        );
        std::thread::yield_now();
    }
    // The waiter's endpoint going away must retract the directory entry even
    // though the first cancel frame is dropped in flight.
    client_runtime
        .send_to(
            proxy,
            NamespaceClientIn::Cancel {
                reply_to: *open_replies.addr(),
            },
        )
        .expect("cancel parked open");
    loop {
        let node = future::block_on(directory_harness.client.lookup(path("/runs/11/parked")))
            .expect("stream node survives the cancel");
        if !node.active {
            break;
        }
        assert!(
            Instant::now() < bound,
            "a lost cancel frame must not strand the pending endpoint forever"
        );
        std::thread::yield_now();
    }
    future::block_on(
        directory_harness
            .client
            .unregister(path("/runs/11/parked"), OperationId::from_u128(32)),
    )
    .expect("quiescent parked stream unlinks after cancel");
}

#[test]
fn stream_retractions_outlive_the_request_deadline() {
    let state = TempState::new("retraction-deadline");
    let directory_harness = spawn_directory(&state.store());
    let directory = directory_harness.client.directory();

    let client_parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let client_runtime = client_parts.runtime().clone();
    let client_engine = Engine::new(
        client_parts,
        TokioBackend::new(TokioConfig {
            worker_threads: 1,
            ..TokioConfig::default()
        })
        .expect("tokio backend"),
    )
    .expect("client engine");
    client_runtime.set_remote_sink(Arc::new(ForwardingSink {
        destination: directory_harness.runtime.clone(),
    }));
    directory_harness
        .runtime
        .set_remote_sink(Arc::new(ForwardingSink {
            destination: client_runtime.clone(),
        }));

    let discovered = Arc::new(RwLock::new(None));
    let proxy = client_runtime
        .spawn(NamespaceClientActor::new_with_deadline(
            client_engine.handle(),
            client_runtime.create_sender(),
            Arc::new(StaticDiscovery {
                directory: Arc::clone(&discovered),
            }),
            Duration::from_millis(20),
            // Ordinary requests must expire within this bound; stream
            // retractions must outlive it.
            Duration::from_millis(150),
        ))
        .expect("spawn namespace proxy");
    let open_replies = client_runtime
        .new_inbox::<NamespaceClientIn>()
        .expect("open reply inbox");
    let parked = path("/runs/13/parked");
    let operation = OperationId::from_u128(41);

    // Park a sink open while no directory is discoverable, then cancel it:
    // the cancel must survive an authority outage far longer than the
    // request deadline and still tombstone the open once the authority
    // returns. Without that survival a reordered late replay of the open
    // resurrects a pending endpoint nobody will ever close.
    client_runtime
        .send_to(
            proxy,
            NamespaceClientIn::Request {
                request: NamespaceRequest::OpenStream {
                    path: parked.clone(),
                    role: StreamRole::Sink,
                    endpoint: source(41),
                    descriptor: Vec::new(),
                    replace: false,
                    ensure: true,
                    expected_revision: None,
                    operation_id: operation,
                },
                reply_to: *open_replies.addr(),
            },
        )
        .expect("send parked sink open");
    client_runtime
        .send_to(
            proxy,
            NamespaceClientIn::Cancel {
                reply_to: *open_replies.addr(),
            },
        )
        .expect("cancel parked open");

    // Outage: several request-deadline lifetimes with retry ticks flowing.
    let settle = Instant::now() + Duration::from_millis(700);
    while Instant::now() < settle {
        client_runtime
            .send_to(proxy, NamespaceClientIn::Retry)
            .expect("drive retry tick");
        std::thread::yield_now();
    }
    *discovered.write() = Some(directory);

    // Give the deadline-surviving retraction clear time to reach the
    // directory and persist its tombstone (the engine tick plus a wide
    // margin), then prove the tombstone exists by replaying the cancelled
    // open: it must be rejected, never resurrected as a pending endpoint.
    let settle_authority = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < settle_authority {
        client_runtime
            .send_to(proxy, NamespaceClientIn::Retry)
            .expect("drive retry tick");
        std::thread::yield_now();
    }
    assert!(
        matches!(
            future::block_on(directory_harness.client.lookup(parked.clone())),
            Err(NamespaceError::PathNotFound(_))
        ),
        "a deadline-surviving cancel leaves no stream node behind"
    );

    // The tombstone must reject a reordered replay of the same open.
    let replies = directory_harness
        .runtime
        .new_inbox::<NamespaceClientIn>()
        .expect("reply inbox");
    directory_harness
        .runtime
        .send_to(
            directory,
            DataDirectoryIn::OpenStream {
                request_id: DirectoryRequestId(77),
                path: parked.clone(),
                role: StreamRole::Sink,
                descriptor: Vec::new(),
                endpoint: source(41),
                replace: false,
                ensure: true,
                expected_revision: None,
                operation_id: operation,
                reply_to: *replies.addr(),
            },
        )
        .expect("send late open replay");
    match future::block_on(replies.recv()) {
        NamespaceClientIn::DirectoryReply(DataDirectoryOut::StreamOpened {
            result: Err(NamespaceError::PathReplaced(_)),
            ..
        }) => {}
        other => panic!("late open replay must hit the cancel tombstone, got {other:?}"),
    }
}

#[test]
fn expired_stream_open_retracts_its_parked_endpoint() {
    let state = TempState::new("expired-open-retract");
    let directory_harness = spawn_directory(&state.store());
    let directory = directory_harness.client.directory();

    let client_parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let client_runtime = client_parts.runtime().clone();
    let client_engine = Engine::new(
        client_parts,
        TokioBackend::new(TokioConfig {
            worker_threads: 1,
            ..TokioConfig::default()
        })
        .expect("tokio backend"),
    )
    .expect("client engine");
    client_runtime.set_remote_sink(Arc::new(ForwardingSink {
        destination: directory_harness.runtime.clone(),
    }));
    directory_harness
        .runtime
        .set_remote_sink(Arc::new(ForwardingSink {
            destination: client_runtime.clone(),
        }));

    let discovered = Arc::new(RwLock::new(Some(directory)));
    let proxy = client_runtime
        .spawn(NamespaceClientActor::new_with_deadline(
            client_engine.handle(),
            client_runtime.create_sender(),
            Arc::new(StaticDiscovery {
                directory: Arc::clone(&discovered),
            }),
            Duration::from_millis(20),
            // The open below parks without a reply; it must expire within
            // this bound and its retraction must still reach the directory.
            Duration::from_millis(150),
        ))
        .expect("spawn namespace proxy");
    // Callers of the proxy receive the raw directory reply (the proxy
    // unwraps NamespaceClientIn::DirectoryReply before forwarding).
    let open_replies = client_runtime
        .new_inbox::<DataDirectoryOut>()
        .expect("open reply inbox");
    let parked = path("/runs/17/parked");
    let operation = OperationId::from_u128(51);

    // A first-role stream open that reaches the directory registers its
    // endpoint and then parks: no reply exists until the peer role arrives.
    // This is the gated-stream shape that strands in production — the sink
    // never comes, the caller's request deadline fires while the endpoint
    // is committed in the directory.
    client_runtime
        .send_to(
            proxy,
            NamespaceClientIn::Request {
                request: NamespaceRequest::OpenStream {
                    path: parked.clone(),
                    role: StreamRole::Source,
                    endpoint: source(51),
                    descriptor: Vec::new(),
                    replace: false,
                    ensure: true,
                    expected_revision: None,
                    operation_id: operation,
                },
                reply_to: *open_replies.addr(),
            },
        )
        .expect("send parked source open");
    let bound = Instant::now() + Duration::from_secs(10);
    loop {
        let active = matches!(
            future::block_on(directory_harness.client.lookup(parked.clone())),
            Ok(node) if node.active
        );
        if active {
            break;
        }
        assert!(
            Instant::now() < bound,
            "parked source open never reached the directory"
        );
        std::thread::yield_now();
    }
    // Nobody cancels and no sink arrives: drive retry ticks past the request
    // deadline. The caller must observe the typed deadline failure...
    let expire = Instant::now() + Duration::from_millis(700);
    while Instant::now() < expire {
        client_runtime
            .send_to(proxy, NamespaceClientIn::Retry)
            .expect("drive retry tick");
        std::thread::yield_now();
    }
    match future::block_on(open_replies.recv()) {
        DataDirectoryOut::StreamOpened {
            result: Err(NamespaceError::DirectoryUnavailable(_)),
            ..
        } => {}
        other => panic!("parked open must expire at the deadline, got {other:?}"),
    }

    // ...and the expiry must retract the parked endpoint so the path can be
    // unlinked. Pre-fix, the endpoint survived forever and unregister
    // failed with WrongEntryType (the campaign's permanent-ENXIO wedge).
    let retract = Instant::now() + Duration::from_millis(1500);
    loop {
        let inactive = matches!(
            future::block_on(directory_harness.client.lookup(parked.clone())),
            Ok(node) if !node.active
        );
        if inactive {
            break;
        }
        assert!(
            Instant::now() < retract,
            "expired open must retract its parked endpoint"
        );
        client_runtime
            .send_to(proxy, NamespaceClientIn::Retry)
            .expect("drive retry tick");
        std::thread::yield_now();
    }
    future::block_on(
        directory_harness
            .client
            .unregister(parked, OperationId::from_u128(52)),
    )
    .expect("quiescent expired stream unlinks");
}

#[test]
fn unresolved_request_waits_for_recovered_authority() {
    let state = TempState::new("restart");
    let harness = spawn_directory(&state.store());
    let logical = path("/models/restartable");
    let source = source(44);
    future::block_on(harness.client.register(
        logical.clone(),
        source,
        [9; 32],
        16,
        recovery(source),
        OperationId::from_u128(500),
    ))
    .expect("seed durable binding");
    let old_directory = harness.client.directory();
    harness.runtime.stop_actor(old_directory).unwrap();

    let discovered = Arc::new(RwLock::new(None));
    let proxy = harness
        .runtime
        .spawn(NamespaceClientActor::new(
            harness.engine.handle(),
            harness.runtime.create_sender(),
            Arc::new(StaticDiscovery {
                directory: Arc::clone(&discovered),
            }),
            Duration::from_millis(5),
        ))
        .expect("spawn namespace client");
    let client = NamespaceClient::new(harness.runtime.clone(), proxy);

    future::block_on(async {
        let mut resolving = Box::pin(client.resolve(logical.clone()));
        assert!(
            future::poll_once(resolving.as_mut()).await.is_none(),
            "resolve must remain pending while authority is absent"
        );

        let recovered = DataDirectoryActor::recover(
            state.store(),
            Some(RetirementRetry::new(
                harness.engine.handle(),
                harness.runtime.create_sender(),
                Duration::from_millis(50),
                None,
            )),
            |record, _length| match record {
                SourceRecovery::Actor { actor, node, .. } => Ok((*actor, *node)),
                SourceRecovery::File { path } => Err(NamespaceError::SourceRecovery(format!(
                    "test cannot recover file source {}",
                    path.display()
                ))),
            },
        )
        .expect("recover directory state");
        let recovered = harness
            .runtime
            .spawn(recovered)
            .expect("spawn recovered directory");
        *discovered.write() = Some(recovered);
        harness
            .runtime
            .send_to(proxy, NamespaceClientIn::Retry)
            .expect("trigger rediscovery retry");

        let binding = resolving.await.expect("resolve after authority recovery");
        assert_eq!(binding.source, source);
        assert_eq!(binding.length, 16);
        assert_eq!(binding.revision, 1);
    });
}

#[test]
fn unresolved_request_fails_bounded_when_authority_is_lost() {
    let discovered = Arc::new(RwLock::new(None));
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig {
            worker_threads: 1,
            ..TokioConfig::default()
        })
        .expect("tokio backend"),
    )
    .expect("client engine");
    let proxy = runtime
        .spawn(NamespaceClientActor::new_with_deadline(
            engine.handle(),
            runtime.create_sender(),
            Arc::new(StaticDiscovery {
                directory: Arc::clone(&discovered),
            }),
            Duration::from_millis(5),
            Duration::from_millis(150),
        ))
        .expect("spawn namespace client");
    let client = NamespaceClient::new(runtime.clone(), proxy);

    let outcome = future::block_on(async {
        let bound = Instant::now() + Duration::from_secs(5);
        let mut resolving = Box::pin(client.resolve(path("/models/lost")));
        loop {
            if let Some(outcome) = future::poll_once(resolving.as_mut()).await {
                return outcome;
            }
            assert!(
                Instant::now() < bound,
                "authority loss must fail the request within the deadline, not hang"
            );
            std::thread::yield_now();
        }
    });
    assert!(
        matches!(outcome, Err(NamespaceError::DirectoryUnavailable(_))),
        "expected a bounded directory-unavailable failure, got {outcome:?}"
    );
}

#[test]
fn namespace_process_restart_helper() {
    let Ok(mode) = std::env::var("SWACTOR_NAMESPACE_HELPER_MODE") else {
        return;
    };
    let store = PathBuf::from(
        std::env::var_os("SWACTOR_NAMESPACE_HELPER_STORE").expect("helper store path"),
    );
    let directory = spawn_directory(&store);
    let logical = path("/models/process-restart");
    match mode.as_str() {
        "write" => {
            let actor = source(55);
            let receipt = future::block_on(directory.client.register(
                logical,
                actor,
                [9; 32],
                32,
                recovery(actor),
                OperationId::from_u128(900),
            ))
            .expect("helper durable registration");
            assert_eq!(receipt.revision, 1);
        }
        "read" => {
            let binding =
                future::block_on(directory.client.resolve(logical)).expect("helper recovery");
            assert_eq!(binding.source, source(55));
            assert_eq!(binding.length, 32);
            assert_eq!(binding.revision, 1);
        }
        other => panic!("unknown namespace helper mode {other}"),
    }
}

#[test]
fn namespace_client_routes_remote_transfer_failures_to_local_binding() {
    let state = TempState::new("transfer-failure");
    let harness = spawn_directory(&state.store());
    let proxy = harness
        .runtime
        .spawn(NamespaceClientActor::new(
            harness.engine.handle(),
            harness.runtime.create_sender(),
            Arc::new(StaticDiscovery {
                directory: Arc::new(RwLock::new(Some(harness.client.directory()))),
            }),
            Duration::from_millis(5),
        ))
        .unwrap();
    let destination = harness
        .runtime
        .new_inbox::<data_plane::blob_transfer::BlobTransferEvent>()
        .unwrap();
    let transfer_id = data_plane::blob_transfer::BlobTransferId(71);
    harness
        .runtime
        .send_to(
            proxy,
            NamespaceClientIn::TransferFailed {
                destination: *destination.addr(),
                transfer_id,
                reason: "injected source failure".to_owned(),
            },
        )
        .unwrap();
    assert_eq!(
        future::block_on(destination.recv()),
        data_plane::blob_transfer::BlobTransferEvent::Failed {
            transfer_id,
            reason: "injected source failure".to_owned(),
        }
    );
}

#[derive(Clone, Debug)]
struct ModelBinding {
    source: ActorAddress,
    length: u64,
    revision: u64,
}

#[derive(Clone, Debug)]
enum TypedModelEntry {
    Blob(ModelBinding),
    Stream(Option<data_plane::namespace::StreamMatch>),
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 8,
        max_shrink_iters: 128,
        ..ProptestConfig::default()
    })]

    #[test]
    fn legal_action_sequences_preserve_namespace_guarantees(actions in prop::collection::vec(any::<u8>(), 1..48)) {
        let state = TempState::new("stateful");
        let directory = spawn_directory(&state.store());
        let paths = [path("/models/a"), path("/models/b"), path("/runs/7/result")];
        let mut model = BTreeMap::<DataPath, ModelBinding>::new();
        let mut next_revision = 1_u64;
        let mut next_operation = 1_u128;

        for (step, action) in actions.into_iter().enumerate() {
            let logical = paths[usize::from(action) % paths.len()].clone();
            match action % 4 {
                0 | 1 => {
                    let actor = source(action.wrapping_add(step as u8).wrapping_add(1));
                    let length = u64::from(action) + 1;
                    let operation = OperationId::from_u128(next_operation);
                    next_operation += 1;
                    let receipt = future::block_on(directory.client.register(
                        logical.clone(),
                        actor,
                        [9; 32],
                        length,
                        recovery(actor),
                        operation,
                    )).expect("model registration");
                    prop_assert_eq!(receipt.revision, next_revision);
                    model.insert(logical, ModelBinding { source: actor, length, revision: next_revision });
                    next_revision += 1;
                }
                2 if model.contains_key(&logical) => {
                    let operation = OperationId::from_u128(next_operation);
                    next_operation += 1;
                    let receipt = future::block_on(directory.client.unregister(logical.clone(), operation))
                        .expect("model unregistration");
                    prop_assert_eq!(receipt.revision, next_revision);
                    model.remove(&logical);
                    next_revision += 1;
                }
                _ => {
                    let observed = future::block_on(directory.client.resolve(logical.clone()));
                    match model.get(&logical) {
                        Some(expected) => {
                            let observed = observed.expect("model binding must resolve");
                            prop_assert_eq!(observed.source, expected.source);
                            prop_assert_eq!(observed.length, expected.length);
                            prop_assert_eq!(observed.revision, expected.revision);
                        }
                        None => prop_assert!(matches!(
                            observed,
                            Err(NamespaceError::PathNotFound(found)) if found == logical
                        )),
                    }
                }
            }

            for (path, expected) in &model {
                let observed = future::block_on(directory.client.resolve(path.clone()))
                    .expect("all model bindings remain resolvable");
                prop_assert_eq!(observed.source, expected.source);

                prop_assert_eq!(observed.length, expected.length);
                prop_assert_eq!(observed.revision, expected.revision);
            }
        }
    }
    #[test]
    fn typed_binding_action_strings_match_reference_model(actions in prop::collection::vec(any::<u8>(), 1..64)) {
        let state = TempState::new("typed-stateful");
        let directory = spawn_directory(&state.store());
        let paths = [path("/state/a"), path("/state/b"), path("/state/c")];
        let mut model = BTreeMap::<DataPath, TypedModelEntry>::new();
        let mut next_operation = 10_000_u128;

        for (step, action) in actions.into_iter().enumerate() {
            let logical = paths[usize::from(action) % paths.len()].clone();
            match action % 4 {
                0 => {
                    let actor = source(action.wrapping_add(step as u8).wrapping_add(1));
                    let length = u64::from(action) + 1;
                    let receipt = future::block_on(directory.client.register(
                        logical.clone(),
                        actor,
                        [9; 32],
                        length,
                        recovery(actor),
                        OperationId::from_u128(next_operation),
                    )).expect("blob rebind");
                    next_operation += 1;
                    model.insert(logical, TypedModelEntry::Blob(ModelBinding {
                        source: actor,
                        length,
                        revision: receipt.revision,
                    }));
                }
                1 => {
                    if let Some(TypedModelEntry::Stream(binding)) = model.get_mut(&logical)
                        && let Some(binding) = binding.take()
                    {
                        future::block_on(
                            directory
                                .client
                                .close_stream(logical.clone(), binding.incarnation),
                        )
                        .expect("quiesce prior stream before the next legal replacement");
                    }
                    let source_actor = source(action.wrapping_add(41));
                    let sink_actor = source(action.wrapping_add(97));
                    let mut source_open = Box::pin(directory.client.replace_with_stream(
                        logical.clone(),
                        StreamRole::Source,
                        source_actor,
                        OperationId::from_u128(next_operation),
                    ));
                    next_operation += 1;
                    prop_assert!(future::block_on(future::poll_once(source_open.as_mut())).is_none());
                    let sink_match = future::block_on(directory.client.open_stream(
                        logical.clone(),
                        StreamRole::Sink,
                        sink_actor,
                        OperationId::from_u128(next_operation),
                    )).expect("sink match");
                    next_operation += 1;
                    let source_match = future::block_on(source_open).expect("source match");
                    prop_assert_eq!(&source_match, &sink_match);
                    model.insert(logical, TypedModelEntry::Stream(Some(sink_match)));
                }
                2 => {
                    if let Some(TypedModelEntry::Stream(binding)) = model.get_mut(&logical)
                        && let Some(binding) = binding.take()
                    {
                        future::block_on(directory.client.close_stream(
                            logical.clone(),
                            binding.incarnation,
                        )).expect("close current stream");
                    }
                }
                _ => {
                    let observed = future::block_on(directory.client.resolve(logical.clone()));
                    match model.get(&logical) {
                        Some(TypedModelEntry::Blob(expected)) => {
                            let observed = observed.expect("blob resolves");
                            prop_assert_eq!(observed.source, expected.source);
                            prop_assert_eq!(observed.length, expected.length);
                            prop_assert_eq!(observed.revision, expected.revision);
                        }
                        Some(TypedModelEntry::Stream(_)) => {
                            let wrong_type = matches!(
                                observed,
                                Err(NamespaceError::WrongEntryType {
                                    expected: EntryKind::Blob,
                                    found: EntryKind::Stream,
                                    ..
                                })
                            );
                            prop_assert!(wrong_type, "blob lookup must reject a stream binding");
                        }
                        None => prop_assert!(matches!(
                            observed,
                            Err(NamespaceError::PathNotFound(found)) if found == logical
                        )),
                    }
                }
            }
        }
    }
}

#[test]
fn retirement_retry_redrives_until_acknowledged() {
    struct RetireProbe {
        retire_count: Arc<AtomicU64>,
        acknowledge: Arc<AtomicBool>,
        acknowledgement_count: Arc<AtomicU64>,
    }

    impl swactor::actor::ActorInterface for RetireProbe {
        type Incoming = data_plane::source::BlobSourceIn;
        type Response = ();

        fn handle(&mut self, ctx: &swactor::actor::Ctx<'_>, message: Self::Incoming) {
            if let data_plane::source::BlobSourceIn::Retire { reply_to } = message
                && let Some(reply_to) = reply_to
            {
                self.retire_count.fetch_add(1, Ordering::SeqCst);
                if self.acknowledge.load(Ordering::SeqCst) {
                    if ctx
                        .send(
                            reply_to,
                            DataDirectoryIn::SourceRetired {
                                source: ctx.self_addr(),
                            },
                        )
                        .is_ok()
                    {
                        self.acknowledgement_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            }
        }
    }

    let state = TempState::new("retire-retry");
    let harness = spawn_directory(&state.store());
    let retire_count = Arc::new(AtomicU64::new(0));
    let acknowledge = Arc::new(AtomicBool::new(false));
    let acknowledgement_count = Arc::new(AtomicU64::new(0));
    let probe = harness
        .runtime
        .spawn(RetireProbe {
            retire_count: Arc::clone(&retire_count),
            acknowledge: Arc::clone(&acknowledge),
            acknowledgement_count: Arc::clone(&acknowledgement_count),
        })
        .expect("spawn retire probe");

    let logical = path("/models/retire-retry");
    future::block_on(harness.client.register(
        logical.clone(),
        probe,
        [9; 32],
        8,
        recovery(probe),
        OperationId::from_u128(1),
    ))
    .expect("register probe source");
    future::block_on(
        harness
            .client
            .unregister(logical.clone(), OperationId::from_u128(2)),
    )
    .expect("unregister probe source");

    // No other directory traffic follows the unregister: only the periodic
    // retry tick may re-deliver Retire. Poll past several tick periods.
    let redrive_deadline = Instant::now() + Duration::from_secs(2);
    let mut redriven = 0;
    while retire_count.load(Ordering::SeqCst) < 2 {
        assert!(
            Instant::now() < redrive_deadline,
            "retirement was not re-driven without directory traffic (observed {redriven})"
        );
        redriven = retire_count.load(Ordering::SeqCst);
        std::thread::yield_now();
    }

    // Once the source acknowledges, retries must stop. A tick can already
    // be in flight when the acknowledgement lands, so require the count to
    // stay unchanged for a full quiet window rather than stopping at the
    // first acknowledgement.
    acknowledge.store(true, Ordering::SeqCst);
    let settle_deadline = Instant::now() + Duration::from_secs(2);
    while acknowledgement_count.load(Ordering::SeqCst) == 0 {
        assert!(
            Instant::now() < settle_deadline,
            "acknowledged Retire was never observed after enabling replies"
        );
        std::thread::yield_now();
    }
    let mut last = retire_count.load(Ordering::SeqCst);
    loop {
        let quiet_until = Instant::now() + Duration::from_millis(300);
        while Instant::now() < quiet_until {
            std::thread::yield_now();
        }
        let observed = retire_count.load(Ordering::SeqCst);
        if observed == last {
            break;
        }
        last = observed;
        assert!(
            Instant::now() < settle_deadline,
            "retirement retries never stopped after SourceRetired (observed {observed})"
        );
    }
}

#[test]
fn retirement_retries_do_not_amplify_with_directory_traffic() {
    struct SilentRetireProbe {
        retire_count: Arc<AtomicU64>,
    }

    impl swactor::actor::ActorInterface for SilentRetireProbe {
        type Incoming = data_plane::source::BlobSourceIn;
        type Response = ();

        fn handle(&mut self, _ctx: &swactor::actor::Ctx<'_>, message: Self::Incoming) {
            if matches!(message, data_plane::source::BlobSourceIn::Retire { .. }) {
                self.retire_count.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    // A directory with no retirement retry tick: every Retire delivery is
    // then attributable to a specific trigger (enqueue, on_start, or — the
    // bug under test — unrelated inbound traffic).
    let state = TempState::new("retire-amplify");
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig {
            worker_threads: 1,
            ..TokioConfig::default()
        })
        .expect("tokio backend"),
    )
    .expect("directory engine");
    let actor = DataDirectoryActor::recover(&state.store(), None, |record, _length| match record {
        SourceRecovery::Actor { actor, node, .. } => Ok((*actor, *node)),
        SourceRecovery::File { path } => Err(NamespaceError::SourceRecovery(format!(
            "test cannot recover file source {}",
            path.display()
        ))),
    })
    .expect("recover directory");
    let directory = runtime.spawn(actor).expect("spawn directory actor");
    let client = DirectoryClient::new(runtime.clone(), directory);

    // Register then unregister several paths so the directory holds several
    // pending retirements aimed at live-but-silent sources.
    const RETIREMENTS: u64 = 4;
    let retire_count = Arc::new(AtomicU64::new(0));
    for index in 0..RETIREMENTS {
        let probe = runtime
            .spawn(SilentRetireProbe {
                retire_count: Arc::clone(&retire_count),
            })
            .expect("spawn silent retire probe");
        let logical = path(&format!("/models/retire-amplify/{index}"));
        future::block_on(client.register(
            logical.clone(),
            probe,
            [9; 32],
            8,
            recovery(probe),
            OperationId::from_u128(u128::from(index) + 1),
        ))
        .expect("register probe source");
        future::block_on(
            client.unregister(logical, OperationId::from_u128(100 + u128::from(index))),
        )
        .expect("unregister probe source");
    }

    // Discard the initial delivery (queue_retirement sends immediately).
    let baseline = {
        let settle = Instant::now() + Duration::from_secs(2);
        loop {
            let quiet_until = Instant::now() + Duration::from_millis(20);
            while Instant::now() < quiet_until {
                std::thread::yield_now();
            }
            let observed = retire_count.load(Ordering::SeqCst);
            if observed >= RETIREMENTS {
                break observed;
            }
            assert!(
                Instant::now() < settle,
                "initial Retire deliveries never arrived (observed {observed})"
            );
        }
    };
    let quiet_until = Instant::now() + Duration::from_millis(100);
    while Instant::now() < quiet_until {
        std::thread::yield_now();
    }
    let observed = retire_count.load(Ordering::SeqCst);
    assert_eq!(
        observed, baseline,
        "retirements were re-driven without any trigger"
    );

    // A burst of unrelated read traffic must not multiply pending
    // retirements: every lookup re-fanned every retirement before the fix,
    // coupling the outbound Retire frame rate to the inbound request rate.
    const LOOKUPS: u64 = 64;
    for _ in 0..LOOKUPS {
        let _ = future::block_on(client.lookup(path("/models/absent")));
    }
    let quiet_until = Instant::now() + Duration::from_millis(100);
    while Instant::now() < quiet_until {
        std::thread::yield_now();
    }
    let observed = retire_count.load(Ordering::SeqCst);
    assert_eq!(
        observed,
        baseline,
        "directory traffic amplified retirement re-sends: {LOOKUPS} lookups drove \
         {} extra Retire deliveries for {RETIREMENTS} pending retirements",
        observed - baseline
    );
    drop(engine);
}

#[derive(Default)]
struct Routes {
    callbacks: parking_lot::Mutex<HashMap<ActorAddress, Weak<dyn Fn() + Send + Sync>>>,
    routable: parking_lot::Mutex<Option<HashSet<ActorAddress>>>,
}

impl HostRouteRegistrar for Routes {
    fn register_child(&self, _: ActorAddress, _: [u8; 32]) -> Result<(), String> {
        Ok(())
    }

    fn revoke_child(&self, _: ActorAddress) -> Result<(), String> {
        Ok(())
    }
    fn is_routable(&self, actor: ActorAddress) -> bool {
        self.routable
            .lock()
            .as_ref()
            .is_none_or(|routable| routable.contains(&actor))
    }

    fn watch_route(
        &self,
        actor: ActorAddress,
        changed: Arc<dyn Fn() + Send + Sync>,
    ) -> Option<HostRouteWatch> {
        self.callbacks
            .lock()
            .insert(actor, Arc::downgrade(&changed));
        Some(HostRouteWatch::new(changed))
    }
}

fn settle(backend: &SteppingBackend) {
    for _ in 0..32 {
        backend.step();
    }
}

#[test]
fn retirement_route_wakes_are_targeted_and_end_on_acknowledgement() {
    use data_plane::source::BlobSourceIn;

    let state = TempState::new("retire-route-wake");
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).unwrap();
    let routes = Arc::new(Routes::default());
    let period = Duration::from_millis(250);
    let directory = runtime
        .spawn(
            DataDirectoryActor::recover(
                state.store(),
                Some(RetirementRetry::new(
                    engine.handle(),
                    runtime.create_sender(),
                    period,
                    Some(routes.clone()),
                )),
                |record, _| match record {
                    SourceRecovery::Actor { actor, node, .. } => Ok((*actor, *node)),
                    SourceRecovery::File { .. } => unreachable!(),
                },
            )
            .unwrap(),
        )
        .unwrap();
    let replies = runtime.new_inbox::<NamespaceClientIn>().unwrap();
    let sources = [
        runtime.new_inbox::<BlobSourceIn>().unwrap(),
        runtime.new_inbox::<BlobSourceIn>().unwrap(),
    ];
    for (index, source) in sources.iter().enumerate() {
        let logical = path(&format!("/retire/{index}"));
        runtime
            .send_to(
                directory,
                DataDirectoryIn::Register {
                    request_id: DirectoryRequestId(index as u64),
                    path: logical.clone(),
                    source: *source.addr(),
                    source_node: [9; 32],
                    length: 1,
                    recovery: recovery(*source.addr()),
                    operation_id: OperationId::from_u128(index as u128 * 2 + 1),
                    reservation: None,
                    reply_to: *replies.addr(),
                },
            )
            .unwrap();
        runtime
            .send_to(
                directory,
                DataDirectoryIn::Unregister {
                    request_id: DirectoryRequestId(index as u64 + 2),
                    path: logical,
                    operation_id: OperationId::from_u128(index as u128 * 2 + 2),
                    reply_to: *replies.addr(),
                },
            )
            .unwrap();
    }
    settle(&backend);
    for source in &sources {
        assert!(matches!(
            source.try_recv(),
            Some(BlobSourceIn::Retire { .. })
        ));
        assert!(source.try_recv().is_none());
    }

    // A ready route re-drives its obligation without moving the virtual clock,
    // and must not fan out the other pending retirement.
    let wake = routes.callbacks.lock()[sources[0].addr()]
        .upgrade()
        .unwrap();
    wake();
    drop(wake);
    settle(&backend);
    assert!(matches!(
        sources[0].try_recv(),
        Some(BlobSourceIn::Retire { .. })
    ));
    assert!(sources[1].try_recv().is_none());
    runtime
        .send_to(
            directory,
            DataDirectoryIn::SourceRetired {
                source: *sources[0].addr(),
            },
        )
        .unwrap();
    settle(&backend);
    assert!(
        routes.callbacks.lock()[sources[0].addr()]
            .upgrade()
            .is_none()
    );

    backend.advance_time(period);
    settle(&backend);
    assert!(sources[0].try_recv().is_none());
    assert!(matches!(
        sources[1].try_recv(),
        Some(BlobSourceIn::Retire { .. })
    ));
    assert!(sources[1].try_recv().is_none());
}

#[test]
fn retirement_ticks_skip_unroutable_sources_until_route_wake() {
    use data_plane::source::BlobSourceIn;

    let state = TempState::new("retire-unroutable");
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).unwrap();
    let routes = Arc::new(Routes::default());
    *routes.routable.lock() = Some(HashSet::new());
    let period = Duration::from_millis(250);
    let directory = runtime
        .spawn(
            DataDirectoryActor::recover(
                state.store(),
                Some(RetirementRetry::new(
                    engine.handle(),
                    runtime.create_sender(),
                    period,
                    Some(routes.clone()),
                )),
                |record, _| match record {
                    SourceRecovery::Actor { actor, node, .. } => Ok((*actor, *node)),
                    SourceRecovery::File { .. } => unreachable!(),
                },
            )
            .unwrap(),
        )
        .unwrap();
    let replies = runtime.new_inbox::<NamespaceClientIn>().unwrap();
    let source = runtime.new_inbox::<BlobSourceIn>().unwrap();
    let logical = path("/retire/unroutable");
    runtime
        .send_to(
            directory,
            DataDirectoryIn::Register {
                request_id: DirectoryRequestId(1),
                path: logical.clone(),
                source: *source.addr(),
                source_node: [9; 32],
                length: 1,
                recovery: recovery(*source.addr()),
                operation_id: OperationId::from_u128(1),
                reservation: None,
                reply_to: *replies.addr(),
            },
        )
        .unwrap();
    runtime
        .send_to(
            directory,
            DataDirectoryIn::Unregister {
                request_id: DirectoryRequestId(2),
                path: logical,
                operation_id: OperationId::from_u128(2),
                reply_to: *replies.addr(),
            },
        )
        .unwrap();
    settle(&backend);
    assert!(matches!(
        source.try_recv(),
        Some(BlobSourceIn::Retire { .. })
    ));
    assert!(source.try_recv().is_none());

    for _ in 0..4 {
        backend.advance_time(period);
        settle(&backend);
    }
    assert!(
        source.try_recv().is_none(),
        "periodic retries must remain dormant while the route is absent"
    );

    routes
        .routable
        .lock()
        .as_mut()
        .unwrap()
        .insert(*source.addr());
    let wake = routes.callbacks.lock()[source.addr()].upgrade().unwrap();
    wake();
    drop(wake);
    settle(&backend);
    assert!(matches!(
        source.try_recv(),
        Some(BlobSourceIn::Retire { .. })
    ));
}

#[test]
fn retirement_retry_ticks_are_bounded_and_rotate_fairly() {
    use data_plane::source::BlobSourceIn;

    let state = TempState::new("retire-batch");
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).unwrap();
    let routes = Arc::new(Routes::default());
    let period = Duration::from_millis(250);
    let directory = runtime
        .spawn(
            DataDirectoryActor::recover(
                state.store(),
                Some(RetirementRetry::new(
                    engine.handle(),
                    runtime.create_sender(),
                    period,
                    Some(routes),
                )),
                |record, _| match record {
                    SourceRecovery::Actor { actor, node, .. } => Ok((*actor, *node)),
                    SourceRecovery::File { .. } => unreachable!(),
                },
            )
            .unwrap(),
        )
        .unwrap();
    let replies = runtime.new_inbox::<NamespaceClientIn>().unwrap();
    let sources = (0..40)
        .map(|index| {
            let source = runtime.new_inbox::<BlobSourceIn>().unwrap();
            let logical = path(&format!("/retire/batch/{index}"));
            runtime
                .send_to(
                    directory,
                    DataDirectoryIn::Register {
                        request_id: DirectoryRequestId(index * 2),
                        path: logical.clone(),
                        source: *source.addr(),
                        source_node: [9; 32],
                        length: 1,
                        recovery: recovery(*source.addr()),
                        operation_id: OperationId::from_u128(index as u128 * 2 + 1),
                        reservation: None,
                        reply_to: *replies.addr(),
                    },
                )
                .unwrap();
            runtime
                .send_to(
                    directory,
                    DataDirectoryIn::Unregister {
                        request_id: DirectoryRequestId(index * 2 + 1),
                        path: logical,
                        operation_id: OperationId::from_u128(index as u128 * 2 + 2),
                        reply_to: *replies.addr(),
                    },
                )
                .unwrap();
            source
        })
        .collect::<Vec<_>>();
    settle(&backend);
    for source in &sources {
        assert!(matches!(
            source.try_recv(),
            Some(BlobSourceIn::Retire { .. })
        ));
        assert!(source.try_recv().is_none());
    }

    let mut seen = vec![false; sources.len()];
    backend.advance_time(period);
    settle(&backend);
    let mut first_tick = 0;
    for (index, source) in sources.iter().enumerate() {
        if source.try_recv().is_some() {
            seen[index] = true;
            first_tick += 1;
        }
    }
    assert_eq!(
        first_tick, 32,
        "one retry tick must have a fixed fanout bound"
    );

    backend.advance_time(period);
    settle(&backend);
    let mut second_tick = 0;
    for (index, source) in sources.iter().enumerate() {
        if source.try_recv().is_some() {
            seen[index] = true;
            second_tick += 1;
        }
    }
    assert_eq!(
        second_tick, 32,
        "the next retry tick retains the same bound"
    );
    assert!(
        seen.into_iter().all(|received| received),
        "the retry cursor must reach every pending source"
    );
    assert!(
        sources.iter().all(|source| source.try_recv().is_none()),
        "one retry frame per selected source is expected"
    );
}

#[test]
fn displacement_acknowledgements_retry_durability_after_endpoints_stop() {
    use data_plane::namespace::StreamIncarnation;
    use data_plane::namespace_store::NamespaceStore;
    use data_plane::protocol::HostStreamIn;
    use data_plane::source::BlobSourceIn;
    use swactor::actor::{ActorInterface, Ctx};

    struct OneShotEndpoint {
        incarnation: StreamIncarnation,
        stopped: Arc<AtomicU64>,
    }

    impl ActorInterface for OneShotEndpoint {
        type Incoming = HostStreamIn;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx<'_>, message: Self::Incoming) {
            if let HostStreamIn::Displaced {
                incarnation,
                reply_to: Some(reply_to),
            } = message
            {
                assert_eq!(incarnation, self.incarnation);
                ctx.send(
                    reply_to,
                    DataDirectoryIn::StreamDisplaced {
                        endpoint: ctx.self_addr(),
                        incarnation,
                    },
                )
                .unwrap();
                ctx.stop_self();
            }
        }

        fn on_stop(&mut self, _: &Ctx<'_>) {
            self.stopped.fetch_add(1, Ordering::SeqCst);
        }
    }

    let state = TempState::new("displacement-durable-ack");
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    let backend = SteppingBackend::new();
    let engine = Engine::new(parts, backend.clone()).unwrap();
    let routes = Arc::new(Routes::default());
    let period = Duration::from_millis(250);
    let stopped = Arc::new(AtomicU64::new(0));
    let incarnation = StreamIncarnation {
        authority_epoch: 1,
        revision: 1,
    };
    let endpoints = [(); 2].map(|()| {
        runtime
            .spawn(OneShotEndpoint {
                incarnation,
                stopped: stopped.clone(),
            })
            .unwrap()
    });
    let unrelated_source = runtime.new_inbox::<BlobSourceIn>().unwrap();
    let replies = runtime.new_inbox::<NamespaceClientIn>().unwrap();
    let logical = path("/unrelated/stream");

    // Recover real persisted obligations, including an unrelated unacknowledged
    // retirement that must keep progressing while stream completion is blocked.
    let mut store = NamespaceStore::open(state.store()).unwrap();
    let mut snapshot = store.snapshot().clone();
    snapshot.authority_epoch = incarnation.authority_epoch;
    snapshot.next_revision = incarnation.revision + 1;
    snapshot
        .stream_nodes
        .insert(logical.clone(), incarnation.revision);
    snapshot.stream_retirements = endpoints
        .iter()
        .map(|endpoint| (*endpoint, incarnation))
        .collect();
    snapshot.retirements.push(*unrelated_source.addr());
    store.commit(snapshot).unwrap();
    drop(store);
    let actor = DataDirectoryActor::recover(
        state.store(),
        Some(RetirementRetry::new(
            engine.handle(),
            runtime.create_sender(),
            period,
            Some(routes.clone()),
        )),
        |_, _| unreachable!("fixture has no blob bindings"),
    )
    .unwrap();

    // Filesystem failpoint: a directory cannot be replaced by the store file.
    // Arm after recovery, before the endpoints receive their displacement, so
    // every ACK completion commit fails without a process-global test hook.
    let saved_store = state.root.join("last-durable-namespace.json");
    std::fs::rename(state.store(), &saved_store).unwrap();
    std::fs::create_dir(state.store()).unwrap();
    let directory = runtime.spawn(actor).unwrap();
    settle(&backend);
    assert_eq!(stopped.load(Ordering::SeqCst), 2);
    for endpoint in endpoints {
        assert!(routes.callbacks.lock()[&endpoint].upgrade().is_some());
    }
    assert!(matches!(
        unrelated_source.try_recv(),
        Some(BlobSourceIn::Retire { .. })
    ));

    runtime
        .send_to(
            directory,
            DataDirectoryIn::Lookup {
                request_id: DirectoryRequestId(1),
                path: logical,
                reply_to: *replies.addr(),
            },
        )
        .unwrap();
    backend.advance_time(period);
    settle(&backend);
    assert!(matches!(
        replies.try_recv(),
        Some(NamespaceClientIn::DirectoryReply(DataDirectoryOut::LookedUp {
            request_id: DirectoryRequestId(1),
            result: Ok(node),
            ..
        })) if node.kind == EntryKind::Stream && node.revision == incarnation.revision
    ));
    assert!(matches!(
        unrelated_source.try_recv(),
        Some(BlobSourceIn::Retire { .. })
    ));
    for endpoint in endpoints {
        assert!(routes.callbacks.lock()[&endpoint].upgrade().is_some());
    }

    std::fs::remove_dir(state.store()).unwrap();
    std::fs::rename(&saved_store, state.store()).unwrap();

    // A targeted route wake completes only its received ACK, locally. Both
    // endpoints have stopped and cannot supply another acknowledgement.
    let source_wake = routes.callbacks.lock()[&endpoints[0]].clone();
    source_wake.upgrade().unwrap()();
    settle(&backend);
    assert!(source_wake.upgrade().is_none());
    assert!(routes.callbacks.lock()[&endpoints[1]].upgrade().is_some());
    assert_eq!(
        NamespaceStore::open(state.store())
            .unwrap()
            .snapshot()
            .stream_retirements,
        vec![(endpoints[1], incarnation)]
    );
    assert!(unrelated_source.try_recv().is_none());

    // The unchanged periodic retry also retries durability, not a dead peer.
    backend.advance_time(period);
    settle(&backend);
    let completed = NamespaceStore::open(state.store()).unwrap();
    assert!(completed.snapshot().stream_retirements.is_empty());
    assert_eq!(
        completed.snapshot().retirements,
        vec![*unrelated_source.addr()]
    );
    assert!(routes.callbacks.lock()[&endpoints[1]].upgrade().is_none());
    assert!(matches!(
        unrelated_source.try_recv(),
        Some(BlobSourceIn::Retire { .. })
    ));
}
