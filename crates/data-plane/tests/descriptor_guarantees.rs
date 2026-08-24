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

    std::thread::scope(|scope| {
        for index in 0_u8..16 {
            let data_plane = data_plane.clone();
            scope.spawn(move || {
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
            });
        }
    });

    std::thread::scope(|scope| {
        for index in 0_u8..16 {
            let data_plane = data_plane.clone();
            scope.spawn(move || {
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
            });
        }
    });
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
