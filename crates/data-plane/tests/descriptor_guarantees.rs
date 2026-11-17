#![cfg(target_os = "linux")]

include!("data_plane_test_support.inc");

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use proptest::prelude::*;

struct ThreadCountingAllocator;

thread_local! {
    static COUNT_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
    static ALLOCATION_COUNT: Cell<usize> = const { Cell::new(0) };
}

#[global_allocator]
static COUNTING_ALLOCATOR: ThreadCountingAllocator = ThreadCountingAllocator;

unsafe impl GlobalAlloc for ThreadCountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        COUNT_ALLOCATIONS.with(|enabled| {
            if enabled.get() {
                ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
            }
        });
        // SAFETY: this allocator delegates the unchanged layout to `System`.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        // SAFETY: `pointer` came from `System` with this layout.
        unsafe { System.dealloc(pointer, layout) };
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        COUNT_ALLOCATIONS.with(|enabled| {
            if enabled.get() {
                ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
            }
        });
        // SAFETY: this allocator delegates the unchanged layout to `System`.
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        COUNT_ALLOCATIONS.with(|enabled| {
            if enabled.get() {
                ALLOCATION_COUNT.with(|count| count.set(count.get() + 1));
            }
        });
        // SAFETY: `pointer` and `layout` came from `System`.
        unsafe { System.realloc(pointer, layout, size) }
    }
}

fn start_allocation_count() {
    ALLOCATION_COUNT.with(|count| count.set(0));
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(true));
}

fn finish_allocation_count() -> usize {
    COUNT_ALLOCATIONS.with(|enabled| enabled.set(false));
    ALLOCATION_COUNT.with(Cell::get)
}

const REQUIRED_MUTATION_TARGETS: &[(&str, &str)] = &[
    (
        "omit_access_check",
        "forbidden_actions_report_stable_errno_and_do_not_mutate_live_state",
    ),
    (
        "advance_requested_count",
        "generated_blob_sequences_preserve_exact_tagged_prefixes",
    ),
    (
        "early_stream_eof",
        "raw_stream_descriptor_hides_record_boundaries_and_preserves_eof",
    ),
    (
        "release_partial_record",
        "partial_record_cursor_keeps_capacity_pinned_until_full_release",
    ),
    (
        "duplicate_stream_prefix",
        "raw_stream_descriptor_hides_record_boundaries_and_preserves_eof",
    ),
    (
        "cross_wire_open_grant",
        "concurrent_descriptor_opens_keep_paths_and_payloads_correlated",
    ),
    (
        "publish_aborted_blob",
        "legal_blob_sequences_cover_boundaries_offsets_mapping_close_and_abort",
    ),
    (
        "reclaim_live_mapping",
        "raw_blob_mapping_is_bounded_and_can_outlive_descriptor_close",
    ),
    (
        "writable_read_only_export",
        "test_raw_descriptor_blob_io_mapping_and_errno",
    ),
    (
        "retain_cancelled_waiter",
        "cancellation_and_transport_faults_reclaim_waiters_and_preserve_unrelated_progress",
    ),
    (
        "double_publication",
        "raw_blob_descriptor_enforces_offsets_rights_and_terminal_state",
    ),
    (
        "authorize_unresolved_alias",
        "missing_and_unauthorized_paths_are_rejected",
    ),
    (
        "silently_stage_direct_map",
        "raw_blob_mapping_is_bounded_and_can_outlive_descriptor_close",
    ),
    (
        "touch_outside_region",
        "generated_blob_sequences_preserve_exact_tagged_prefixes",
    ),
    (
        "leak_failed_open_lease",
        "cancelled_write_open_releases_queued_grant",
    ),
];

#[test]
fn mutation_adequacy_targets_cover_every_required_bad_behavior() {
    let mut identifiers: Vec<&str> = REQUIRED_MUTATION_TARGETS
        .iter()
        .map(|(identifier, _)| *identifier)
        .collect();
    assert_eq!(identifiers.len(), 15);
    identifiers.sort_unstable();
    identifiers.dedup();
    assert_eq!(identifiers.len(), 15, "mutation identifiers must be unique");
    assert!(
        REQUIRED_MUTATION_TARGETS
            .iter()
            .all(|(_, oracle)| !oracle.is_empty())
    );
}

#[test]
fn legal_blob_sequences_cover_boundaries_offsets_mapping_close_and_abort() {
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();

    future::block_on(async {
        for (case, length) in [0_usize, 1, 7, 64].into_iter().enumerate() {
            let logical = path(&format!("/runs/self/results/legal-{case}"));
            let payload: Vec<u8> = (0..length).map(|index| index as u8).collect();
            let mut writer = data_plane
                .open(&logical, OpenOptions::staged_blob(length as u64))
                .await
                .expect("legal staged open");
            for chunk in payload.chunks(3) {
                writer.write_all(chunk).await.expect("legal partial write");
            }
            writer.close().await.expect("legal publication");

            let mut reader = data_plane
                .open(&logical, OpenOptions::read_only())
                .await
                .expect("legal read open");
            let mapping = reader
                .map(MapRequest {
                    protection: Protection::Read,
                    sharing: Sharing::Shared,
                    target: MapTarget::Host,
                    offset: 0,
                    length: length as u64,
                })
                .expect("legal read mapping");
            assert_eq!(mapping.as_ref(), payload);
            assert_eq!(mapping.route(), TransferRoute::Direct);

            let mut observed = Vec::new();
            let mut destination = [0xa5_u8; 11];
            loop {
                let count = reader
                    .read(&mut destination[..(case + 1).min(11)])
                    .await
                    .expect("legal sequential read");
                if count == 0 {
                    break;
                }
                observed.extend_from_slice(&destination[..count]);
            }
            assert_eq!(observed, payload);
            assert_eq!(reader.read(&mut destination).await.expect("sticky eof"), 0);
            reader.close().await.expect("legal reader close");
        }

        let aborted = path("/runs/self/results/legal-abort");
        let mut writer = data_plane
            .open(&aborted, OpenOptions::staged_blob(4))
            .await
            .expect("abort candidate");
        writer.write_all(b"nope").await.expect("staged bytes");
        writer.abort().await.expect("explicit abort");
        assert_eq!(
            data_plane
                .open(&aborted, OpenOptions::read_only())
                .await
                .expect_err("aborted blob is absent")
                .errno(),
            Errno::Enoent
        );
    });
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(8))]

    #[test]
    fn generated_blob_sequences_preserve_exact_tagged_prefixes(
        payload in prop::collection::vec(any::<u8>(), 0..96),
        write_size in 1_usize..17,
        read_size in 1_usize..17,
    ) {
        let harness = harness(2 << 20);
        let data_plane = harness.bootstrap.data_plane.clone();
        future::block_on(async {
            let logical = path("/runs/self/results/property-sequence");
            let mut writer = data_plane
                .open(&logical, OpenOptions::staged_blob(payload.len() as u64))
                .await
                .expect("property writer");
            for chunk in payload.chunks(write_size) {
                writer.write_all(chunk).await.expect("property write");
            }
            writer.close().await.expect("property publish");

            let mut reader = data_plane
                .open(&logical, OpenOptions::read_only())
                .await
                .expect("property reader");
            let mut destination = vec![0xa5_u8; read_size + 2];
            let mut observed = Vec::new();
            loop {
                destination.fill(0xa5);
                let count = reader
                    .read(&mut destination[1..=read_size])
                    .await
                    .expect("property read");
                assert_eq!(destination[0], 0xa5);
                assert_eq!(destination[read_size + 1], 0xa5);
                if count == 0 {
                    break;
                }
                observed.extend_from_slice(&destination[1..1 + count]);
            }
            // Kills: advance offset by requested rather than completed bytes,
            // touch destination outside the returned prefix, duplicate/drop data.
            prop_assert_eq!(observed, payload);
            Ok(())
        })?;
    }
}

#[test]
fn forbidden_actions_report_stable_errno_and_do_not_mutate_live_state() {
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/negative");

    future::block_on(async {
        let mut writer = data_plane
            .open(&logical, OpenOptions::staged_blob(4))
            .await
            .expect("negative fixture writer");
        writer.write_all(b"safe").await.expect("fixture bytes");
        assert_eq!(
            writer
                .read(&mut [0_u8; 1])
                .await
                .expect_err("wrong access")
                .errno(),
            Errno::Ebadf
        );
        writer.close().await.expect("fixture publication");

        let invalid = OpenOptions {
            exclusive: true,
            ..OpenOptions::default()
        };
        assert_eq!(
            data_plane
                .open(&logical, invalid)
                .await
                .expect_err("invalid flags")
                .errno(),
            Errno::Einval
        );
        assert_eq!(
            data_plane
                .open(
                    &logical,
                    OpenOptions {
                        nonblocking: true,
                        ..OpenOptions::default()
                    },
                )
                .await
                .expect_err("unsupported nonblocking")
                .errno(),
            Errno::Enotsup
        );
        assert_eq!(
            data_plane
                .open(&path("/models/missing-raw"), OpenOptions::read_only())
                .await
                .expect_err("missing path")
                .errno(),
            Errno::Enoent
        );
        assert_eq!(
            data_plane
                .open(
                    &path("/models/tiny-linear/weights"),
                    OpenOptions::staged_blob(1),
                )
                .await
                .expect_err("unauthorized write")
                .errno(),
            Errno::Eacces
        );
        assert_eq!(
            data_plane
                .open(
                    &logical,
                    OpenOptions {
                        exclusive: true,
                        ..OpenOptions::staged_blob(4)
                    },
                )
                .await
                .expect_err("exclusive existing")
                .errno(),
            Errno::Eexist
        );

        let mut reader = data_plane
            .open(&logical, OpenOptions::read_only())
            .await
            .expect("negative fixture reader");
        assert_eq!(
            reader
                .map(MapRequest {
                    protection: Protection::Read,
                    sharing: Sharing::Shared,
                    target: MapTarget::Host,
                    offset: 3,
                    length: 2,
                })
                .expect_err("mapping overrun")
                .errno(),
            Errno::Einval
        );
        assert_eq!(
            reader
                .write(b"x")
                .await
                .expect_err("read-only write")
                .errno(),
            Errno::Ebadf
        );
        let mut bytes = [0_u8; 4];
        reader
            .read_exact(&mut bytes)
            .await
            .expect("state unchanged");
        assert_eq!(&bytes, b"safe");
        reader.close().await.expect("first close");
        assert_eq!(
            reader.close().await.expect_err("double close").errno(),
            Errno::Ebadf
        );
        // Kills: omitted access checks, mutation after failed bounds checks,
        // silent flag downgrade, and revival after close.
    });
}

#[test]
fn exclusive_create_is_atomic_and_abort_releases_its_reservation() {
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/exclusive-race");
    let options = OpenOptions {
        exclusive: true,
        ..OpenOptions::staged_blob(4)
    };

    future::block_on(async {
        let first = data_plane.open(&logical, options.clone());
        let second = data_plane.open(&logical, options.clone());
        let (first, second) = future::zip(first, second).await;
        let (mut winner, loser) = match (first, second) {
            (Ok(winner), Err(loser)) | (Err(loser), Ok(winner)) => (winner, loser),
            (Ok(_), Ok(_)) => panic!("both exclusive creators succeeded"),
            (Err(first), Err(second)) => {
                panic!("both exclusive creators failed: {first}; {second}")
            }
        };
        assert_eq!(loser.errno(), Errno::Eexist);
        winner.abort().await.expect("abort exclusive winner");

        let mut replacement = data_plane
            .open(&logical, options)
            .await
            .expect("reservation released after abort");
        replacement
            .abort()
            .await
            .expect("abort replacement reservation");
    });
}

#[test]
fn raw_stream_abort_completes_before_peer_observes_broken_pipe() {
    let _stream_test = STREAM_TEST_LOCK.lock();
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let logical = path("/runs/self/results/raw-broken-pipe");

    future::block_on(async {
        let seed_reader = data_plane.read_stream(&logical);
        let seed_writer = data_plane.write_stream(&logical);
        let (seed_reader, seed_writer) = future::zip(seed_reader, seed_writer).await;
        let mut seed_reader = seed_reader.expect("seed reader");
        let mut seed_writer = seed_writer.expect("seed writer");
        seed_writer.close().await.expect("seed writer close");
        assert_eq!(seed_reader.read().await.expect("seed eof"), None);

        let reader = data_plane.open(&logical, OpenOptions::read_only());
        let writer = data_plane.open(
            &logical,
            OpenOptions {
                access: AccessMode::WriteOnly,
                ..OpenOptions::default()
            },
        );
        let (reader, writer) = future::zip(reader, writer).await;
        let mut reader = reader.expect("raw reader");
        let mut writer = writer.expect("raw writer");
        reader.abort().await.expect("reader abort completion");
        assert_eq!(
            writer
                .write(b"late")
                .await
                .expect_err("write after reader abort")
                .errno(),
            Errno::Epipe
        );
    });
}

#[test]
fn concurrent_descriptor_opens_keep_paths_and_payloads_correlated() {
    let harness = harness(8 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();

    let blocking = harness._host_engine.handle().blocking_work_sender();
    let mut writers = Vec::with_capacity(16);
    for index in 0_u8..16 {
        let data_plane = data_plane.clone();
        writers.push(
            blocking
                .submit_joinable(move || {
                    future::block_on(async move {
                        let logical = path(&format!("/runs/self/results/concurrent-{index}"));
                        let payload = [index; 32];
                        let mut writer = data_plane
                            .open(&logical, OpenOptions::staged_blob(payload.len() as u64))
                            .await
                            .expect("concurrent writer");
                        writer
                            .write_all(&payload)
                            .await
                            .expect("concurrent payload");
                        writer.close().await.expect("concurrent publish");
                    });
                })
                .expect("submit concurrent writer"),
        );
    }
    for writer in writers {
        writer.join().expect("concurrent writer completed");
    }

    let mut readers = Vec::with_capacity(16);
    for index in 0_u8..16 {
        let data_plane = data_plane.clone();
        readers.push(
            blocking
                .submit_joinable(move || {
                    future::block_on(async move {
                        let logical = path(&format!("/runs/self/results/concurrent-{index}"));
                        let mut reader = data_plane
                            .open(&logical, OpenOptions::read_only())
                            .await
                            .expect("concurrent reader");
                        let mut payload = [0_u8; 32];
                        reader
                            .read_exact(&mut payload)
                            .await
                            .expect("correlated read");
                        assert_eq!(payload, [index; 32]);
                    });
                })
                .expect("submit concurrent reader"),
        );
    }
    for reader in readers {
        reader.join().expect("concurrent reader completed");
    }
    // Kills: accepting a grant for the wrong operation/path or crossing leases.
}

#[test]
fn cancellation_and_transport_faults_reclaim_waiters_and_preserve_unrelated_progress() {
    let _stream_test = STREAM_TEST_LOCK.lock();
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    let cancelled_path = path("/runs/self/results/cancelled-open");

    future::block_on(async {
        let mut cancelled = Box::pin(data_plane.read_stream(&cancelled_path));
        assert!(future::poll_once(cancelled.as_mut()).await.is_none());
        drop(cancelled);
        std::thread::sleep(Duration::from_millis(10));

        let reader = data_plane.read_stream(&cancelled_path);
        let writer = data_plane.write_stream(&cancelled_path);
        let (reader, writer) = future::zip(reader, writer).await;
        let mut reader = reader.expect("replacement reader");
        let mut writer = writer.expect("replacement writer");
        writer.close().await.expect("replacement close");
        assert_eq!(reader.read().await.expect("replacement eof"), None);

        let mut unrelated = data_plane
            .open(&path("/models/second"), OpenOptions::read_only())
            .await
            .expect("unrelated progress");
        let mut bytes = [0_u8; 11];
        unrelated
            .read_exact(&mut bytes)
            .await
            .expect("unrelated read");
        assert_eq!(&bytes, b"second-blob");
    });

    let fault_harness = harness_with_transport(2 << 20, Arc::new(RejectingStreamTransport));
    let fault_plane = fault_harness.bootstrap.data_plane.clone();
    let fault_path = path("/runs/self/results/transport-fault");
    future::block_on(async {
        let mut reader_open = Box::pin(fault_plane.read_stream(&fault_path));
        assert!(future::poll_once(reader_open.as_mut()).await.is_none());
        let writer_error = match fault_plane.write_stream(&fault_path).await {
            Ok(_) => panic!("faulted writer opened"),
            Err(error) => error,
        };
        let reader_error = match reader_open.await {
            Ok(_) => panic!("faulted reader opened"),
            Err(error) => error,
        };
        assert!(matches!(
            reader_error,
            DataPlaneError::PeerLost | DataPlaneError::StreamFault(_)
        ));
        assert!(matches!(
            writer_error,
            DataPlaneError::PeerLost | DataPlaneError::StreamFault(_)
        ));

        let mut unrelated = fault_plane
            .open(&path("/models/second"), OpenOptions::read_only())
            .await
            .expect("fault isolation");
        let mut bytes = [0_u8; 11];
        unrelated
            .read_exact(&mut bytes)
            .await
            .expect("fault-isolated read");
        assert_eq!(&bytes, b"second-blob");
    });
    // Kills: retain a cancelled waiter, complete only one matched endpoint,
    // and propagate one descriptor fault into unrelated operations.
}

#[test]
fn steady_state_blob_primitives_allocate_no_heap_memory() {
    let harness = harness(2 << 20);
    let data_plane = harness.bootstrap.data_plane.clone();
    future::block_on(async {
        let logical = path("/runs/self/results/allocation-count");
        let mut writer = data_plane
            .open(&logical, OpenOptions::staged_blob(64))
            .await
            .expect("allocation writer");
        let payload = [7_u8; 64];
        start_allocation_count();
        let result = writer.write(&payload).await;
        let write_allocations = finish_allocation_count();
        assert_eq!(result.expect("allocation-count write"), payload.len());
        assert_eq!(write_allocations, 0, "blob write primitive allocated");
        writer.close().await.expect("allocation publish");

        let mut reader = data_plane
            .open(&logical, OpenOptions::read_only())
            .await
            .expect("allocation reader");
        let mut destination = [0_u8; 64];
        start_allocation_count();
        let result = reader.read(&mut destination).await;
        let read_allocations = finish_allocation_count();
        assert_eq!(result.expect("allocation-count read"), destination.len());
        assert_eq!(read_allocations, 0, "blob read primitive allocated");
        assert_eq!(destination, payload);
    });
}

mod accumulated_session_lifecycle {
    use super::*;
    use data_plane::data_plane::{
        DataPlane, HostSessionRouteObserver, HostSessionRouteWatch, StreamConsumer,
    };
    use data_plane::namespace::{
        DataDirectoryActor, DirectoryClient, NamespaceClient, NamespaceClientActor,
    };
    use data_plane::protocol::ChildSessionIn;
    use data_plane::stream_transport::{
        LocalStreamTransport, StreamPeerDescriptor, StreamSinkRequest, StreamSourceRequest,
        StreamTransport, StreamTransportEvent, StreamTransportNotifier,
    };
    use protocol_telemetry::{ActorType, Outcome, Phase, Protocol, Reason, TransitionEmitter};
    use std::any::Any;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};
    use swactor_engine::SteppingBackend;

    type Notification = (Arc<dyn StreamTransportNotifier>, StreamTransportEvent);

    #[derive(Default)]
    struct ReadinessGate(parking_lot::Mutex<Vec<Notification>>);

    impl ReadinessGate {
        fn release(&self, reverse: bool) {
            let mut pending = std::mem::take(&mut *self.0.lock());
            if reverse {
                pending.reverse();
            }
            for (notifier, event) in pending {
                notifier.notify(event);
            }
        }
    }

    struct DelayedNotifier {
        target: Arc<dyn StreamTransportNotifier>,
        gate: Arc<ReadinessGate>,
    }

    impl StreamTransportNotifier for DelayedNotifier {
        fn notify(&self, event: StreamTransportEvent) {
            if event == StreamTransportEvent::Ready {
                self.gate.0.lock().push((self.target.clone(), event));
            } else {
                self.target.notify(event);
            }
        }
    }

    struct ControlledTransport {
        local: LocalStreamTransport,
        gate: Arc<ReadinessGate>,
    }

    impl StreamTransport for ControlledTransport {
        fn descriptor(&self) -> Result<StreamPeerDescriptor, String> {
            self.local.descriptor()
        }
        fn install_source(&self, mut request: StreamSourceRequest) -> Result<(), String> {
            request.notifier = Arc::new(DelayedNotifier {
                target: request.notifier,
                gate: self.gate.clone(),
            });
            self.local.install_source(request)
        }
        fn install_sink(&self, mut request: StreamSinkRequest) -> Result<(), String> {
            request.notifier = Arc::new(DelayedNotifier {
                target: request.notifier,
                gate: self.gate.clone(),
            });
            self.local.install_sink(request)
        }
        fn source_progress(&self, incarnation: data_plane::namespace::StreamIncarnation) {
            self.local.source_progress(incarnation);
        }
        fn sink_progress(&self, incarnation: data_plane::namespace::StreamIncarnation) {
            self.local.sink_progress(incarnation);
        }
        fn source_has_capacity(
            &self,
            incarnation: data_plane::namespace::StreamIncarnation,
        ) -> bool {
            self.local.source_has_capacity(incarnation)
        }
        fn sink_has_data(&self, incarnation: data_plane::namespace::StreamIncarnation) -> bool {
            self.local.sink_has_data(incarnation)
        }
        fn terminate(&self, incarnation: data_plane::namespace::StreamIncarnation) {
            self.local.terminate(incarnation);
        }
    }

    type Delivery = (ActorAddress, Box<dyn Any + Send>);

    struct ForwardingGate {
        destination: Runtime,
        reader: parking_lot::Mutex<Option<ActorAddress>>,
        pending: parking_lot::Mutex<Vec<Delivery>>,
        duplicates: parking_lot::Mutex<Vec<(ActorAddress, ChildSessionIn)>>,
    }

    impl ForwardingGate {
        fn release(&self) {
            for (address, message) in std::mem::take(&mut *self.duplicates.lock()) {
                self.destination.send_to(address, message).unwrap();
            }
            for (address, message) in std::mem::take(&mut *self.pending.lock()) {
                let duplicate = message.downcast_ref::<ChildSessionIn>().unwrap().clone();
                self.duplicates.lock().push((address, duplicate));
                self.destination.deliver_raw(address, message).unwrap();
            }
        }
    }

    impl RemoteSink for ForwardingGate {
        fn send(&self, address: ActorAddress, message: Box<dyn Any + Send>) -> Result<(), Error> {
            if *self.reader.lock() == Some(address)
                && matches!(
                    message.downcast_ref::<ChildSessionIn>(),
                    Some(ChildSessionIn::StreamOpened { .. })
                )
            {
                self.pending.lock().push((address, message));
                Ok(())
            } else {
                self.destination.deliver_raw(address, message)
            }
        }
    }

    struct Steps {
        host: SteppingBackend,
        child: SteppingBackend,
        ready: Arc<ReadinessGate>,
        forwarding: Arc<ForwardingGate>,
    }

    impl Steps {
        fn tick(&self, index: usize, schedule: usize) {
            // Host-only bursts vary namespace matching relative to child open.
            for _ in 0..=schedule % 3 {
                self.host.step();
            }
            if index % (schedule + 1) == 0 {
                self.ready.release(schedule % 2 != 0);
            }
            self.child.step();
            if index % (schedule * 3 + 1) == 0 {
                self.forwarding.release();
            }
        }

        fn run<F: Future>(&self, future: F, schedule: usize) -> F::Output {
            let mut future = pin!(future);
            let mut cx = Context::from_waker(Waker::noop());
            for index in 0..4096 {
                if let Poll::Ready(result) = future.as_mut().poll(&mut cx) {
                    return result;
                }
                self.tick(index, schedule);
            }
            panic!(
                "accepted operation did not resolve in 4096 deterministic steps (schedule {schedule})"
            );
        }

        fn settle(&self) {
            for index in 0..128 {
                self.tick(index, 0);
            }
            assert!(self.ready.0.lock().is_empty());
            assert!(self.forwarding.pending.lock().is_empty());
            assert!(self.forwarding.duplicates.lock().is_empty());
        }
    }

    fn proxy(
        runtime: &Runtime,
        engine: &Engine,
        directory: ActorAddress,
    ) -> (ActorAddress, NamespaceClient) {
        let actor = runtime
            .spawn(NamespaceClientActor::new(
                engine.handle(),
                runtime.create_sender(),
                Arc::new(StaticDiscovery(directory)),
                Duration::from_millis(5),
            ))
            .unwrap();
        (actor, NamespaceClient::new(runtime.clone(), actor))
    }

    fn session(
        host: &Runtime,
        child: &Runtime,
        engine: &Engine,
        steps: &Steps,
        namespace: NamespaceClient,
        transport: Arc<dyn StreamTransport>,
        generation: u64,
        transitions: &TransitionEmitter,
        observer: Option<Arc<dyn HostSessionRouteObserver>>,
    ) -> (ActorAddress, DataPlaneBootstrap) {
        let mut arena = ArenaManager::boot(ArenaConfig {
            node_id: NodeId(generation),
            reservation_ceiling: 2 << 20,
            base_alignment: 64,
        })
        .unwrap();
        let prepared = bootstrap::prepare_arena(
            &mut arena,
            BootstrapSpec {
                arena_generation: generation,
                alignment: 64,
            },
        )
        .unwrap();
        let host_session = host
            .spawn(
                HostDataPlaneSessionActor::new(HostDataPlaneConfig {
                    runtime: host.clone(),
                    engine: engine.handle(),
                    arena,
                    arena_generation: generation,
                    session_generation: generation,
                    capability: CAPABILITY,
                    session_access: SessionAccess {
                        execution_id: "retained".to_owned(),
                        read_prefixes: vec![path("/cases")],
                        write_prefixes: vec![path("/cases")],
                    },
                    namespace: Some(namespace),
                    transfer_receiver: Some(Arc::new(DirectReceiver)),
                    source_sender: Some(Arc::new(LoopbackSender {
                        runtime: host.clone(),
                    })),
                    source_publisher: Some(Arc::new(NoopSourceRegistrar)),
                    route_registrar: None,
                    stream_transport: Some(transport),
                    transitions: Some(transitions.clone()),
                })
                .unwrap(),
            )
            .unwrap();
        let observer = observer.unwrap_or_else(|| {
            Arc::new(RuntimeHostRouteObserver {
                runtime: host.clone(),
            })
        });
        let attached = steps
            .run(
                DataPlaneBootstrap::attach_routed(
                    prepared.arena_fd,
                    child.clone(),
                    host_session,
                    CAPABILITY,
                    None,
                    Some(transitions.clone()),
                    observer,
                ),
                0,
            )
            .unwrap();
        (host_session, attached)
    }

    async fn paired(
        reader: &DataPlane,
        writer: &DataPlane,
        logical: &DataPath,
    ) -> (data_plane::namespace::StreamIncarnation, Vec<u8>) {
        let write = async {
            let mut writer = writer.write_stream(logical).await.unwrap();
            let incarnation = writer.incarnation();
            writer.write(&vec![0x35; 2047]).await.unwrap();
            writer.write(&vec![0x82; 2048]).await.unwrap();
            writer.close().await.unwrap();
            incarnation
        };
        let read = async {
            let mut reader = reader.read_stream(logical).await.unwrap();
            let mut bytes = Vec::new();
            while let Some(frame) = reader.read().await.unwrap() {
                bytes.extend(frame);
            }
            bytes
        };
        future::zip(write, read).await
    }

    struct IgnoreBytes;
    impl StreamConsumer for IgnoreBytes {
        fn consume(&self, _bytes: &[u8]) -> Result<(), String> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct TerminalRoute {
        callbacks: parking_lot::Mutex<Vec<std::sync::Weak<dyn Fn() + Send + Sync>>>,
    }

    impl HostSessionRouteObserver for TerminalRoute {
        fn watch_terminal(
            &self,
            _host: ActorAddress,
            terminal: Arc<dyn Fn() + Send + Sync>,
        ) -> HostSessionRouteWatch {
            self.callbacks.lock().push(Arc::downgrade(&terminal));
            HostSessionRouteWatch::new(terminal)
        }
    }

    impl TerminalRoute {
        fn terminate(&self) {
            for callback in self
                .callbacks
                .lock()
                .iter()
                .filter_map(std::sync::Weak::upgrade)
            {
                callback();
            }
        }
    }

    #[test]
    fn accumulated_restart_reader_open_retains_namespace_and_resolves_across_forwarding_orders() {
        // Reduced fixture from recovery-post-002 attempt 135: retain a published
        // blob and inactive stream across authority/session restart, then repeat
        // 2047+2048-byte paired streams in the SAME recovered namespace.
        // This is not evidence that the historical attempt had the same cause.
        let temp = TempState::new();
        let (host_parts, host) = runtime_parts();
        let (child_parts, child) = runtime_parts();
        let host_backend = SteppingBackend::new();
        let child_backend = SteppingBackend::new();
        let host_engine = Engine::new(host_parts, host_backend.clone()).unwrap();
        let _child_engine = Engine::new(child_parts, child_backend.clone()).unwrap();
        let ready = Arc::new(ReadinessGate::default());
        let forwarding = Arc::new(ForwardingGate {
            destination: child.clone(),
            reader: parking_lot::Mutex::new(None),
            duplicates: parking_lot::Mutex::new(Vec::new()),
            pending: parking_lot::Mutex::new(Vec::new()),
        });
        host.set_remote_sink(forwarding.clone());
        child.set_remote_sink(Arc::new(DirectRuntimeSink {
            destination: host.clone(),
        }));
        let steps = Steps {
            host: host_backend,
            child: child_backend,
            ready: ready.clone(),
            forwarding: forwarding.clone(),
        };
        let transport: Arc<dyn StreamTransport> = Arc::new(ControlledTransport {
            local: LocalStreamTransport::new(),
            gate: ready,
        });
        let endpoint = telemetry::TelemetryEndpoint::with_capacity(
            telemetry::StreamId::new(telemetry::NodeId::new("retained"), telemetry::Lifetime(1)),
            8192,
            8192,
        )
        .with_retention(8192, 4 << 20);
        let transitions = TransitionEmitter::register(&endpoint.producer());
        let store = temp.root.join("retained-namespace.json");
        let source_path = temp.root.join("persisted-blob.bin");
        std::fs::write(&source_path, vec![0x19; 1025]).unwrap();
        let source_actor = data_plane::source::FileBlobSourceActor::open(
            host.clone(),
            Arc::new(LoopbackSender {
                runtime: host.clone(),
            }),
            &source_path,
        )
        .unwrap();
        let recovery = source_actor.recovery();
        let source = host.spawn(source_actor).unwrap();
        let directory = host
            .spawn(DataDirectoryActor::recover(&store, None, |_, _| Ok((source, [9; 32]))).unwrap())
            .unwrap();
        let direct = DirectoryClient::new(host.clone(), directory);
        let blob_path = path("/cases/recovery-pre-002/persisted-blob");
        steps
            .run(
                direct.register(
                    blob_path.clone(),
                    source,
                    [9; 32],
                    1025,
                    recovery,
                    data_plane::namespace::OperationId::from_u128(1),
                ),
                0,
            )
            .unwrap();
        let (old_proxy, namespace) = proxy(&host, &host_engine, directory);
        let (_, old_reader) = session(
            &host,
            &child,
            &host_engine,
            &steps,
            namespace.clone(),
            transport.clone(),
            1,
            &transitions,
            None,
        );
        let (_, old_writer) = session(
            &host,
            &child,
            &host_engine,
            &steps,
            namespace,
            transport.clone(),
            2,
            &transitions,
            None,
        );
        let persisted_stream = path("/cases/recovery-pre-002/persisted-stream");
        let (before_restart, bytes) = steps.run(
            paired(
                &old_reader.data_plane,
                &old_writer.data_plane,
                &persisted_stream,
            ),
            1,
        );
        assert_eq!(bytes, [vec![0x35; 2047], vec![0x82; 2048]].concat());
        steps
            .run(old_reader.data_plane.close_acknowledged(), 0)
            .unwrap();
        steps
            .run(old_writer.data_plane.close_acknowledged(), 0)
            .unwrap();
        steps.settle();
        host.stop_actor(old_proxy).unwrap();
        host.stop_actor(directory).unwrap();
        steps.settle();

        let recovered = host
            .spawn(DataDirectoryActor::recover(&store, None, |_, _| Ok((source, [9; 32]))).unwrap())
            .unwrap();
        let (new_proxy, namespace) = proxy(&host, &host_engine, recovered);
        let (_, reader) = session(
            &host,
            &child,
            &host_engine,
            &steps,
            namespace.clone(),
            transport.clone(),
            3,
            &transitions,
            None,
        );
        let (_, writer) = session(
            &host,
            &child,
            &host_engine,
            &steps,
            namespace,
            transport,
            4,
            &transitions,
            None,
        );
        let persisted = steps
            .run(writer.data_plane.lookup(&persisted_stream), 0)
            .unwrap();
        assert_eq!(persisted.kind, data_plane::namespace::EntryKind::Stream);
        assert!(!persisted.active);
        let blob = steps
            .run(writer.data_plane.read_blob(&blob_path), 0)
            .unwrap();
        assert_eq!(blob.length(), 1025);
        drop(blob);
        steps.settle();
        let baseline = (host.stats().actors.len(), child.stats().actors.len());
        *forwarding.reader.lock() = Some(reader.data_plane.child_session());
        for repetition in 0..12 {
            let logical = path("/cases/recovery-post-002/attempt-134/random/5945560748687097875/2");
            let (incarnation, bytes) = steps.run(
                paired(&reader.data_plane, &writer.data_plane, &logical),
                repetition % 4,
            );
            assert!(incarnation.authority_epoch > before_restart.authority_epoch);
            assert_eq!(bytes, [vec![0x35; 2047], vec![0x82; 2048]].concat());
            steps.settle();
            let node = steps.run(reader.data_plane.lookup(&logical), 0).unwrap();
            assert!(
                !node.active,
                "iteration {repetition} leaked its namespace endpoint"
            );
            steps.settle();
            assert_eq!(
                (host.stats().actors.len(), child.stats().actors.len()),
                baseline,
                "iteration {repetition} retained an operation or binding after caller completion"
            );
            endpoint.tick();
        }
        // The old session remains a closed tombstone. A caller must receive the
        // actor's actual message type; the old raw Result rejection stranded this
        // consumer despite host/session resources having already been released.
        let rejected = old_reader
            .data_plane
            .collect_stream(persisted_stream.clone(), Arc::new(IgnoreBytes))
            .unwrap();
        steps.settle();
        assert_eq!(
            rejected.wait_deadline(Duration::ZERO),
            Some(Err(DataPlaneError::SessionNotRunning))
        );
        steps
            .run(reader.data_plane.close_acknowledged(), 0)
            .unwrap();
        steps
            .run(writer.data_plane.close_acknowledged(), 0)
            .unwrap();
        steps.settle();
        endpoint.tick();
        let retained = endpoint.subscribe_retained(
            "reader-open-regression",
            telemetry::SubscriptionRequest::all(),
        );
        let mut decoder =
            protocol_telemetry::consumer::TransitionDecoder::from_snapshot(retained.snapshot());
        let records: Vec<_> = retained
            .drain_available()
            .iter()
            .filter_map(|event| {
                decoder
                    .observe(event)
                    .unwrap()
                    .map(|observed| observed.record)
            })
            .collect();
        assert!(records.iter().any(|record| record.actor_type
            == ActorType::ChildDataPlaneSessionActor
            && record.reason == Reason::Forwarded
            && record.incarnation.is_some()));
        assert!(records.iter().any(|record| record.actor_type
            == ActorType::DescriptorOpenOperationActor
            && record.outcome == Some(Outcome::Success)
            && record.incarnation.is_some()));
        for actor_type in [
            ActorType::NamespaceControlActor,
            ActorType::NamespaceOpenLookupActor,
            ActorType::NamespaceResolveActor,
            ActorType::NamespaceStreamOpenActor,
            ActorType::NamespaceStreamCloseActor,
        ] {
            let starts: Vec<_> = records
                .iter()
                .filter(|record| {
                    record.actor_type == actor_type && record.reason == Reason::Started
                })
                .collect();
            assert!(
                !starts.is_empty(),
                "{actor_type:?} lost its session emitter"
            );
            for started in starts {
                assert_eq!(started.protocol, Protocol::NamespaceOperation);
                let terminal: Vec<_> = records
                    .iter()
                    .filter(|record| record.actor == started.actor && record.outcome.is_some())
                    .collect();
                assert_eq!(terminal.len(), 1, "{actor_type:?} completed more than once");
                assert_eq!(terminal[0].outcome, Some(Outcome::Success));
                assert_eq!(terminal[0].previous, Phase::AwaitingReply);
                assert_eq!(terminal[0].next, Phase::Finished);
                assert_eq!(terminal[0].operation, started.operation);
                assert!(terminal[0].operation.is_some());
                assert!(terminal[0].generation.is_some());
            }
        }
        for plane in [
            &old_reader.data_plane,
            &old_writer.data_plane,
            &reader.data_plane,
            &writer.data_plane,
        ] {
            child.stop_actor(plane.child_session()).unwrap();
        }
        host.stop_actor(new_proxy).unwrap();
        host.stop_actor(recovered).unwrap();
        host.stop_actor(source).unwrap();
        steps.settle();
        assert!(host.stats().actors.is_empty());
        assert!(child.stats().actors.is_empty());
    }

    #[test]
    fn session_close_resolves_open_and_rejects_following_descriptor_once() {
        let temp = TempState::new();
        let (host_parts, host) = runtime_parts();
        let (child_parts, child) = runtime_parts();
        let host_backend = SteppingBackend::new();
        let child_backend = SteppingBackend::new();
        let host_engine = Engine::new(host_parts, host_backend.clone()).unwrap();
        let _child_engine = Engine::new(child_parts, child_backend.clone()).unwrap();
        let forwarding = Arc::new(ForwardingGate {
            destination: child.clone(),
            reader: parking_lot::Mutex::new(None),
            pending: parking_lot::Mutex::new(Vec::new()),
            duplicates: parking_lot::Mutex::new(Vec::new()),
        });
        host.set_remote_sink(forwarding.clone());
        child.set_remote_sink(Arc::new(DirectRuntimeSink {
            destination: host.clone(),
        }));
        let steps = Steps {
            host: host_backend,
            child: child_backend,
            ready: Arc::new(ReadinessGate::default()),
            forwarding,
        };
        let directory = host
            .spawn(
                DataDirectoryActor::recover(temp.root.join("close.json"), None, |_, _| {
                    panic!("no persisted sources")
                })
                .unwrap(),
            )
            .unwrap();
        let (proxy, namespace) = proxy(&host, &host_engine, directory);
        let endpoint = telemetry::TelemetryEndpoint::new(telemetry::StreamId::new(
            telemetry::NodeId::new("close"),
            telemetry::Lifetime(1),
        ));
        let transitions = TransitionEmitter::register(&endpoint.producer());
        let (host_session, attached) = session(
            &host,
            &child,
            &host_engine,
            &steps,
            namespace,
            Arc::new(LocalStreamTransport::new()),
            1,
            &transitions,
            None,
        );
        let logical = path("/cases/close/open");
        let mut opening = pin!(attached.data_plane.read_stream(&logical));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(opening.as_mut().poll(&mut cx).is_pending());
        steps.settle();
        steps
            .run(attached.data_plane.close_acknowledged(), 0)
            .unwrap();
        assert!(matches!(
            opening.as_mut().poll(&mut cx),
            Poll::Ready(Err(DataPlaneError::SessionNotRunning))
        ));
        assert!(matches!(
            steps.run(
                attached.data_plane.open(&logical, OpenOptions::read_only()),
                0
            ),
            Err(DataPlaneError::SessionNotRunning)
        ));
        // Repeated terminal observations cannot recreate local work.
        child
            .send_to(
                attached.data_plane.child_session(),
                ChildSessionIn::CloseCompleted { result: Ok(()) },
            )
            .unwrap();
        steps.settle();
        assert!(
            !host
                .stats()
                .actors
                .iter()
                .any(|(actor, _)| *actor == host_session)
        );
        child
            .stop_actor(attached.data_plane.child_session())
            .unwrap();
        host.stop_actor(proxy).unwrap();
        host.stop_actor(directory).unwrap();
        steps.settle();
        assert!(host.stats().actors.is_empty());
        assert!(child.stats().actors.is_empty());
    }

    #[test]
    fn routed_host_terminal_event_resolves_pending_local_callers_and_fences_stale_hosts() {
        let temp = TempState::new();
        let (host_parts, host) = runtime_parts();
        let (child_parts, child) = runtime_parts();
        let host_backend = SteppingBackend::new();
        let child_backend = SteppingBackend::new();
        let host_engine = Engine::new(host_parts, host_backend.clone()).unwrap();
        let _child_engine = Engine::new(child_parts, child_backend.clone()).unwrap();
        let forwarding = Arc::new(ForwardingGate {
            destination: child.clone(),
            reader: parking_lot::Mutex::new(None),
            pending: parking_lot::Mutex::new(Vec::new()),
            duplicates: parking_lot::Mutex::new(Vec::new()),
        });
        host.set_remote_sink(forwarding.clone());
        child.set_remote_sink(Arc::new(DirectRuntimeSink {
            destination: host.clone(),
        }));
        let steps = Steps {
            host: host_backend,
            child: child_backend,
            ready: Arc::new(ReadinessGate::default()),
            forwarding,
        };
        let directory = host
            .spawn(
                DataDirectoryActor::recover(temp.root.join("terminal.json"), None, |_, _| {
                    panic!("no persisted sources")
                })
                .unwrap(),
            )
            .unwrap();
        let (proxy, namespace) = proxy(&host, &host_engine, directory);
        let endpoint = telemetry::TelemetryEndpoint::new(telemetry::StreamId::new(
            telemetry::NodeId::new("terminal"),
            telemetry::Lifetime(1),
        ));
        let transitions = TransitionEmitter::register(&endpoint.producer());
        let observer = Arc::new(TerminalRoute::default());
        let (_, attached) = session(
            &host,
            &child,
            &host_engine,
            &steps,
            namespace,
            Arc::new(LocalStreamTransport::new()),
            1,
            &transitions,
            Some(observer.clone()),
        );
        let logical = path("/cases/terminal/descriptor");
        let mut opening = Box::pin(attached.data_plane.read_stream(&logical));
        let collecting = attached
            .data_plane
            .collect_stream(path("/cases/terminal/collector"), Arc::new(IgnoreBytes))
            .unwrap();
        let mut cx = Context::from_waker(Waker::noop());
        assert!(opening.as_mut().poll(&mut cx).is_pending());
        steps.settle();
        child
            .send_to(
                attached.data_plane.child_session(),
                ChildSessionIn::HostUnavailable {
                    host_session: ActorAddress::new_random(),
                },
            )
            .unwrap();
        steps.settle();
        assert!(
            opening.as_mut().poll(&mut cx).is_pending(),
            "old host identity must not terminate the replacement session"
        );
        observer.terminate();
        steps.settle();
        assert!(matches!(
            opening.as_mut().poll(&mut cx),
            Poll::Ready(Err(DataPlaneError::PeerLost))
        ));
        drop(opening);
        assert_eq!(
            collecting.wait_deadline(Duration::ZERO),
            Some(Err(DataPlaneError::PeerLost))
        );
        observer.terminate();
        steps.settle();
        assert_eq!(
            steps.run(attached.data_plane.close_acknowledged(), 0),
            Err(DataPlaneError::PeerLost)
        );
        child
            .stop_actor(attached.data_plane.child_session())
            .unwrap();
        steps.settle();
        host.stop_actor(proxy).unwrap();
        host.stop_actor(directory).unwrap();
        steps.settle();
        assert!(host.stats().actors.is_empty());
        assert!(child.stats().actors.is_empty());
        drop(attached);
        assert!(
            observer
                .callbacks
                .lock()
                .iter()
                .all(|callback| callback.upgrade().is_none())
        );
    }

    #[test]
    fn remote_attachment_without_terminal_observer_rejects_before_accepting_local_work() {
        let (host_parts, host) = runtime_parts();
        let (child_parts, child) = runtime_parts();
        let host_backend = SteppingBackend::new();
        let child_backend = SteppingBackend::new();
        let host_engine = Engine::new(host_parts, host_backend.clone()).unwrap();
        let _child_engine = Engine::new(child_parts, child_backend).unwrap();
        let mut arena = ArenaManager::boot(ArenaConfig {
            node_id: NodeId(1),
            reservation_ceiling: 1 << 20,
            base_alignment: 64,
        })
        .unwrap();
        let prepared = bootstrap::prepare_arena(
            &mut arena,
            BootstrapSpec {
                arena_generation: 1,
                alignment: 64,
            },
        )
        .unwrap();
        let host_session = host
            .spawn(
                HostDataPlaneSessionActor::new(HostDataPlaneConfig {
                    runtime: host.clone(),
                    engine: host_engine.handle(),
                    arena,
                    arena_generation: 1,
                    session_generation: 1,
                    capability: CAPABILITY,
                    session_access: SessionAccess {
                        execution_id: "observer-required".to_owned(),
                        read_prefixes: Vec::new(),
                        write_prefixes: Vec::new(),
                    },
                    namespace: None,
                    transfer_receiver: None,
                    source_sender: None,
                    source_publisher: None,
                    route_registrar: None,
                    stream_transport: None,
                    transitions: None,
                })
                .unwrap(),
            )
            .unwrap();
        let mut attachment = Box::pin(DataPlaneBootstrap::attach(
            prepared.arena_fd,
            child.clone(),
            host_session,
            CAPABILITY,
            None,
        ));
        let mut cx = Context::from_waker(Waker::noop());
        assert!(matches!(
            attachment.as_mut().poll(&mut cx),
            Poll::Ready(Err(DataPlaneError::InvalidArgument(_)))
        ));
        assert!(
            child.stats().actors.is_empty(),
            "rejected composition must not leave an attaching actor"
        );
        drop(attachment);
        host.stop_actor(host_session).unwrap();
        for _ in 0..32 {
            host_backend.step();
        }
        assert!(host.stats().actors.is_empty());
    }
}
