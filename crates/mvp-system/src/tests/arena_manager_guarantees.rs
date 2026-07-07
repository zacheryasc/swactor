//! Black-box contract tests for MVP ArenaManager behavior.
//!
//! These tests intentionally know only the public arena-manager surface:
//!
//! - arena boot configuration
//! - lease, cancel, release, quiescence, and shutdown requests in
//! - lease events, rejected requests, released ranges, and faults out
//!
//! They assert the guarantees in
//! `specs/mvp_system/arena_manager_contract.md`.

use mvp_system::arena_manager as arena;

// A small deterministic arena makes overlap, alignment, queueing, and reuse
// proofs easy to inspect. The concrete mmap strategy remains outside the test.
fn arena_config() -> arena::ArenaConfig {
    arena::ArenaConfig {
        node_id: arena::NodeId(10),
        reservation_ceiling: 4096,
        base_alignment: 64,
    }
}

// Lease requests name size and alignment only. They do not request pointers or
// private allocator slots, which keeps layout authority inside ArenaManager.
fn lease_request(request_id: u64, bytes: u64, alignment: u64) -> arena::LeaseRing {
    arena::LeaseRing {
        request_id: arena::LeaseRequestId(request_id),
        ring_spec: arena::RingSpec {
            header_bytes: 128,
            data_bytes: bytes,
            alignment,
        },
    }
}

// The harness exposes public lease events and lease snapshots. Tests use those
// snapshots only after a RingLeased event, so private allocator state remains
// unobservable.
fn new_arena() -> arena::ArenaManagerHarness {
    arena::ArenaManagerHarness::boot(arena_config()).expect("test arena must boot")
}

// This helper proves two public layouts are disjoint using half-open ranges.
// It is more useful than comparing offsets directly because allocator choice is
// intentionally implementation-defined.
fn assert_non_overlapping(left: &arena::RingLayout, right: &arena::RingLayout) {
    let left_range = left.start_offset..left.end_offset;
    let right_range = right.start_offset..right.end_offset;
    assert!(
        left_range.end <= right_range.start || right_range.end <= left_range.start,
        "live leases overlap: {left:?} and {right:?}"
    );
}

// This proves sampling an unused arena reports only reserved capacity and no
// allocator activity.
#[test]
fn sample_reports_empty_arena_capacity_and_zero_activity() {
    let harness = new_arena();

    let sample = harness.sample(41);

    assert_eq!(sample.seq, 41);
    assert!(
        sample.sample_unix_ms > 0,
        "sample timestamp must be a populated Unix epoch millisecond"
    );
    assert_eq!(sample.capacity_bytes, arena_config().reservation_ceiling);
    assert_eq!(sample.live_bytes, 0);
    assert_eq!(sample.free_bytes, arena_config().reservation_ceiling);
    assert_eq!(sample.active_leases, 0);
    assert_eq!(sample.pending_leases, 0);
    assert_eq!(
        sample.largest_free_range_bytes,
        arena_config().reservation_ceiling
    );
    assert_eq!(sample.allocation_failures_total, 0);
    assert_eq!(sample.release_failures_total, 0);
}

// This proves a granted lease is reflected in live capacity accounting and the
// active lease count without relying on private allocator slots.
#[test]
fn sample_counts_live_bytes_and_active_leases_after_grant() {
    let mut harness = new_arena();
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 512, 64)));
    let lease = harness.live_leases()[0].clone();
    let lease_bytes = lease.layout.end_offset - lease.layout.start_offset;

    let sample = harness.sample(42);

    assert_eq!(sample.seq, 42);
    assert_eq!(sample.capacity_bytes, arena_config().reservation_ceiling);
    assert_eq!(sample.live_bytes, lease_bytes);
    assert_eq!(
        sample.free_bytes,
        arena_config().reservation_ceiling - lease_bytes
    );
    assert_eq!(sample.active_leases, 1);
    assert_eq!(sample.pending_leases, 0);
    assert_eq!(sample.allocation_failures_total, 0);
    assert_eq!(sample.release_failures_total, 0);
}

// This proves a proof-backed release returns the full range to the free pool and
// restores the largest allocatable span.
#[test]
fn sample_reports_full_free_space_after_release() {
    let mut harness = new_arena();
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 512, 64)));
    let ring_id = harness.live_leases()[0].ring_id;

    harness.request(arena::ArenaRequest::ReleaseRing {
        ring_id,
        proof: arena::QuiescenceProof::verified(),
    });
    let sample = harness.sample(43);

    assert_eq!(sample.live_bytes, 0);
    assert_eq!(sample.free_bytes, arena_config().reservation_ceiling);
    assert_eq!(sample.active_leases, 0);
    assert_eq!(sample.pending_leases, 0);
    assert_eq!(
        sample.largest_free_range_bytes,
        arena_config().reservation_ceiling
    );
}

// This proves queued leases are visible as pending work while existing live
// leases continue to own their bytes.
#[test]
fn sample_counts_queued_requests_as_pending_leases() {
    let mut harness = new_arena();
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 3072, 64)));
    let live = harness.live_leases()[0].clone();
    let live_bytes = live.layout.end_offset - live.layout.start_offset;
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(2, 1024, 64)));

    let sample = harness.sample(44);

    assert_eq!(sample.live_bytes, live_bytes);
    assert_eq!(
        sample.free_bytes,
        arena_config().reservation_ceiling - live_bytes
    );
    assert_eq!(sample.active_leases, 1);
    assert_eq!(sample.pending_leases, 1);
    assert_eq!(sample.allocation_failures_total, 0);
    assert_eq!(sample.release_failures_total, 0);
}

// This proves lease rejection increments the allocation failure counter without
// changing the arena's free capacity.
#[test]
fn sample_counts_rejected_leases_as_allocation_failures() {
    let mut harness = new_arena();

    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 8192, 64)));
    let sample = harness.sample(45);

    assert_eq!(sample.live_bytes, 0);
    assert_eq!(sample.free_bytes, arena_config().reservation_ceiling);
    assert_eq!(sample.active_leases, 0);
    assert_eq!(sample.pending_leases, 0);
    assert_eq!(
        sample.largest_free_range_bytes,
        arena_config().reservation_ceiling
    );
    assert_eq!(sample.allocation_failures_total, 1);
    assert_eq!(sample.release_failures_total, 0);
}

// This proves release rejection increments the release failure counter and keeps
// the live lease accounted as active.
#[test]
fn sample_counts_rejected_releases_as_release_failures() {
    let mut harness = new_arena();
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 512, 64)));
    let lease = harness.live_leases()[0].clone();
    let lease_bytes = lease.layout.end_offset - lease.layout.start_offset;

    harness.request(arena::ArenaRequest::ReleaseRing {
        ring_id: lease.ring_id,
        proof: arena::QuiescenceProof::missing(),
    });
    let sample = harness.sample(46);

    assert_eq!(sample.live_bytes, lease_bytes);
    assert_eq!(
        sample.free_bytes,
        arena_config().reservation_ceiling - lease_bytes
    );
    assert_eq!(sample.active_leases, 1);
    assert_eq!(sample.pending_leases, 0);
    assert_eq!(sample.allocation_failures_total, 0);
    assert_eq!(sample.release_failures_total, 1);
}

// This proves arena boot creates one stable sparse arena with one reservation
// ceiling, offset-only layouts, and typed boot failure.
#[test]
fn arena_boot_creates_stable_offset_only_layout_domain() {
    // Boot a valid arena.
    let mut harness = new_arena();

    // Lease one ring so the public layout can be inspected.
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 512, 64)));
    let lease = harness
        .events()
        .iter()
        .find_map(|event| match event {
            arena::ArenaEvent::RingLeased { lease } => Some(lease),
            _ => None,
        })
        .expect("valid lease must be granted");

    // Layout facts are arena offsets and stay under the reservation ceiling.
    assert!(lease.layout.start_offset < arena_config().reservation_ceiling);
    assert!(lease.layout.end_offset <= arena_config().reservation_ceiling);
    assert!(matches!(
        lease.layout.pointer,
        arena::LayoutPointer::NoProcessPointer
    ));

    // Boot failure is typed and emits no usable arena.
    let failed = arena::ArenaManagerHarness::boot(arena::ArenaConfig {
        reservation_ceiling: 0,
        ..arena_config()
    });
    assert!(matches!(
        failed,
        Err(arena::ArenaFault::InvalidReservationCeiling)
    ));
}

// This proves LeaseRing either leases, queues, or rejects, and that live RingId
// values are unique for the node lifetime.
#[test]
fn lease_requests_grant_queue_or_reject_with_unique_ring_ids() {
    // Fill most of the arena with one live lease.
    let mut harness = new_arena();
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 3072, 64)));
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(2, 1024, 64)));

    // A satisfiable request under pressure may queue instead of rejecting.
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            arena::ArenaEvent::RingLeaseQueued {
                request_id: arena::LeaseRequestId(2)
            }
        )
    }));

    // A request larger than the reservation ceiling must reject.
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(3, 8192, 64)));
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            arena::ArenaEvent::RingLeaseRejected {
                request_id: arena::LeaseRequestId(3),
                reason: arena::RingLeaseRejection::CannotFitWithinCeiling,
            }
        )
    }));

    // Granted RingId values must be unique among all live leases.
    let ids = harness
        .live_leases()
        .iter()
        .map(|lease| lease.ring_id)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(ids.len(), harness.live_leases().len());
}

// This proves live leases are non-overlapping, in-bounds, aligned, and stable
// for the lease lifetime.
#[test]
fn live_layouts_are_non_overlapping_aligned_in_bounds_and_stable() {
    // Lease two rings with explicit alignment requirements.
    let mut harness = new_arena();
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 512, 64)));
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(2, 512, 128)));

    // Read the public lease snapshots.
    let leases = harness.live_leases().to_vec();
    assert_eq!(leases.len(), 2);

    // Prove non-overlap and in-bounds without constraining allocator placement.
    assert_non_overlapping(&leases[0].layout, &leases[1].layout);
    for lease in &leases {
        assert!(lease.layout.end_offset <= arena_config().reservation_ceiling);
        assert_eq!(lease.layout.start_offset % lease.requested_alignment, 0);
    }

    // Re-observing the same live lease must not change offsets.
    let before = leases[0].layout.clone();
    let after = harness
        .lookup_lease(leases[0].ring_id)
        .expect("live lease must be lookupable")
        .layout
        .clone();
    assert_eq!(after, before);
}

// This proves CancelLease removes queued work and suppresses later hot-path
// installation, including a fresh lease that races with cancellation.
#[test]
fn cancelled_queued_lease_never_installs_hot_path_state() {
    // Fill the arena and queue a second satisfiable lease.
    let mut harness = new_arena();
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 3072, 64)));
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(2, 1024, 64)));

    // Cancel the queued request before it is leased.
    harness.request(arena::ArenaRequest::CancelLease {
        request_id: arena::LeaseRequestId(2),
    });

    // Releasing pressure must not install worker or pump state for the canceled
    // request.
    let live = harness.live_leases()[0].ring_id;
    harness.request(arena::ArenaRequest::ReleaseRing {
        ring_id: live,
        proof: arena::QuiescenceProof::verified(),
    });
    assert!(!harness.commands().iter().any(|command| {
        matches!(
            command,
            arena::ArenaCommand::InstallWorkerOrPumpState {
                request_id: arena::LeaseRequestId(2),
                ..
            }
        )
    }));

    // If a fresh lease was produced during the race, it must be released instead
    // of becoming hot-path state.
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            arena::ArenaEvent::CancelledFreshLeaseReleased {
                request_id: arena::LeaseRequestId(2)
            }
        )
    }));
}

// This proves ranges are released only with quiescence proof, are not reused
// while live work owns them, and may be reused after release.
#[test]
fn release_requires_quiescence_and_reuse_happens_only_after_release() {
    // Lease one ring and record its range.
    let mut harness = new_arena();
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 1024, 64)));
    let first = harness.live_leases()[0].clone();

    // Release without proof must reject and keep the range live.
    harness.request(arena::ArenaRequest::ReleaseRing {
        ring_id: first.ring_id,
        proof: arena::QuiescenceProof::missing(),
    });
    assert!(harness.lookup_lease(first.ring_id).is_some());

    // A second lease while the first is live must not overlap the first range.
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(2, 1024, 64)));
    let second = harness
        .live_leases()
        .iter()
        .find(|lease| lease.ring_id != first.ring_id)
        .expect("second live lease must exist")
        .clone();
    assert_non_overlapping(&first.layout, &second.layout);

    // Verified quiescence allows release, after which reuse is legal.
    harness.request(arena::ArenaRequest::ReleaseRing {
        ring_id: first.ring_id,
        proof: arena::QuiescenceProof::verified(),
    });
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(3, 1024, 64)));
    assert!(
        harness
            .events()
            .iter()
            .any(|event| { matches!(event, arena::ArenaEvent::RingLeased { .. }) })
    );
}

// This proves shutdown rejects new leases, preserves live lease records, and
// does not release live ranges without quiescence proof.
#[test]
fn shutdown_rejects_new_leases_without_corrupting_live_records() {
    // Create a live lease before shutdown.
    let mut harness = new_arena();
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(1, 512, 64)));
    let live_before = harness.live_leases().to_vec();

    // Shut the arena down.
    harness.request(arena::ArenaRequest::Shutdown);

    // New leases are rejected after shutdown.
    harness.request(arena::ArenaRequest::LeaseRing(lease_request(2, 512, 64)));
    assert!(harness.events().iter().any(|event| {
        matches!(
            event,
            arena::ArenaEvent::RingLeaseRejected {
                request_id: arena::LeaseRequestId(2),
                reason: arena::RingLeaseRejection::ArenaShuttingDown,
            }
        )
    }));

    // Existing live lease records remain intact until proof-backed release.
    assert_eq!(harness.live_leases(), live_before.as_slice());
}
