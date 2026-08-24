use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use data_plane::namespace::{
    DataDirectoryActor, DirectoryClient, EntryKind, NamespaceClient, NamespaceClientActor,
    NamespaceClientIn, NamespaceDiscovery, NamespaceError, OperationId, SourceRecovery, StreamRole,
};
use data_plane::path::DataPath;
use futures_lite::future;
use parking_lot::RwLock;
use proptest::prelude::*;
use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::{Runtime, RuntimeParts};
use swactor_engine::{Engine, TokioBackend, TokioConfig};

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
    SourceRecovery::Actor { actor }
}

fn spawn_directory(store: &Path) -> DirectoryHarness {
    let parts = RuntimeParts::new(RuntimeConfig {
        worker_count: 1,
        ..RuntimeConfig::default()
    });
    let runtime = parts.runtime().clone();
    let actor = DataDirectoryActor::recover(store, |record, _length| match record {
        SourceRecovery::Actor { actor } => Ok(*actor),
        SourceRecovery::File { path } => Err(NamespaceError::SourceRecovery(format!(
            "test cannot recover file source {}",
            path.display()
        ))),
    })
    .expect("recover directory");
    let directory = runtime.spawn(actor).expect("spawn directory actor");
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig {
            worker_threads: 1,
            ..TokioConfig::default()
        })
        .expect("tokio backend"),
    )
    .expect("directory engine");
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

        let mut sink_open = Box::pin(directory.client.open_stream(
            logical.clone(),
            StreamRole::Sink,
            source(12),
            OperationId::from_u128(12),
        ));
        assert!(future::poll_once(sink_open.as_mut()).await.is_none());
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
fn unresolved_request_waits_for_recovered_authority() {
    let state = TempState::new("restart");
    let harness = spawn_directory(&state.store());
    let logical = path("/models/restartable");
    let source = source(44);
    future::block_on(harness.client.register(
        logical.clone(),
        source,
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

        let recovered =
            DataDirectoryActor::recover(state.store(), |record, _length| match record {
                SourceRecovery::Actor { actor } => Ok(*actor),
                SourceRecovery::File { path } => Err(NamespaceError::SourceRecovery(format!(
                    "test cannot recover file source {}",
                    path.display()
                ))),
            })
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
