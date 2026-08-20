//! Bootstrap ABI guarantees (behavior invariants, not implementation shape).
//!
//! Defends:
//! - B3/B4: the parser rejects every malformed page with a typed error and
//!   never trusts page claims over ground truth.
//! - B8: the writer emits exactly the two environment ABI names.
//! - B10: after `write_bootstrap` returns, the header is readable from the
//!   arena and parseable, the ring region is zeroed with its generation
//!   stamped, and header/ring leases are disjoint by arena placement law.
#![cfg(target_os = "linux")]

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId};
use data_plane::bootstrap::{
    self, parse_bootstrap, BootstrapError, ControlRingLayout, BOOTSTRAP_MAGIC, BOOTSTRAP_VERSION,
    ENV_ARENA_FD, ENV_WAKE_FD, HEADER_END_OFFSET, HEADER_LEN,
};

fn arena() -> ArenaManager {
    ArenaManager::boot(ArenaConfig {
        node_id: NodeId(10),
        reservation_ceiling: 1 << 20,
        base_alignment: 64,
    })
    .expect("test arena must boot")
}

fn spec() -> bootstrap::ControlRingSpec {
    bootstrap::ControlRingSpec {
        data_bytes: 4096,
        alignment: 64,
    }
}

fn valid_header(arena_size: u64) -> data_plane::bootstrap::BootstrapHeader {
    data_plane::bootstrap::BootstrapHeader {
        arena_size,
        control_ring: ControlRingLayout {
            offset: 4096,
            capacity: 8192,
            generation: 1,
        },
    }
}

// ─── Parser ──────────────────────────────────────────────────────────────────

#[test]
fn parser_round_trips_encoded_header() {
    let page = valid_header(1 << 20).encode();
    let resolved = parse_bootstrap(&page, 1 << 20).expect("valid header must parse");
    assert_eq!(resolved.arena_size, 1 << 20);
    assert_eq!(resolved.control_ring, valid_header(1 << 20).control_ring);
}

#[test]
fn parser_rejects_short_pages() {
    let error = parse_bootstrap(&valid_header(1 << 20).encode()[..HEADER_LEN - 1], 1 << 20)
        .expect_err("truncated page must fail");
    assert_eq!(
        error,
        BootstrapError::Truncated {
            available: HEADER_LEN - 1
        }
    );
}

#[test]
fn parser_rejects_bad_magic() {
    let mut page = valid_header(1 << 20).encode();
    page[0] ^= 0xFF;
    let error = parse_bootstrap(&page, 1 << 20).expect_err("bad magic must fail");
    assert_eq!(
        error,
        BootstrapError::BadMagic {
            found: BOOTSTRAP_MAGIC ^ 0xFF
        }
    );
}

#[test]
fn parser_rejects_unsupported_versions_without_guessing() {
    for version in [0_u16, 2, 3, u16::MAX] {
        let mut page = valid_header(1 << 20).encode();
        page[4..6].copy_from_slice(&version.to_le_bytes());
        let error = parse_bootstrap(&page, 1 << 20)
            .expect_err("unsupported version must not parse");
        assert_eq!(
            error,
            BootstrapError::UnsupportedVersion {
                found: version,
                supported: BOOTSTRAP_VERSION
            }
        );
    }
}

#[test]
fn parser_rejects_nonzero_reserved_bytes() {
    for (at, byte) in [(6_usize, 7_u8), (40, 1), (47, 0xFF)] {
        let mut page = valid_header(1 << 20).encode();
        page[at] = byte;
        let error = parse_bootstrap(&page, 1 << 20)
            .expect_err("nonzero reserved bytes must not parse");
        assert_eq!(
            error,
            BootstrapError::ReservedBytesNotZero {
                at: if at < 8 { 6 } else { 40 }
            }
        );
    }
}

#[test]
fn parser_rejects_lying_arena_size() {
    let page = valid_header(1 << 20).encode();
    let error =
        parse_bootstrap(&page, (1 << 20) + 1).expect_err("size disagreement must fail");
    assert_eq!(
        error,
        BootstrapError::ArenaSizeMismatch {
            header: 1 << 20,
            backing: (1 << 20) + 1
        }
    );
}

#[test]
fn parser_rejects_ring_defects() {
    let base = valid_header(1 << 20);
    let cases: Vec<(ControlRingLayout, BootstrapError)> = vec![
        (
            ControlRingLayout {
                offset: HEADER_END_OFFSET - 8,
                ..base.control_ring
            },
            BootstrapError::RingOverlapsHeader {
                offset: HEADER_END_OFFSET - 8,
            },
        ),
        (
            ControlRingLayout {
                offset: 1 << 20,
                capacity: 1,
                ..base.control_ring
            },
            BootstrapError::RingOutOfBounds {
                offset: 1 << 20,
                capacity: 1,
                arena_size: 1 << 20,
            },
        ),
        (
            ControlRingLayout {
                capacity: 0,
                ..base.control_ring
            },
            BootstrapError::ZeroRingCapacity,
        ),
        (
            ControlRingLayout {
                generation: 0,
                ..base.control_ring
            },
            BootstrapError::ZeroRingGeneration,
        ),
    ];
    for (ring, expected) in cases {
        let page = data_plane::bootstrap::BootstrapHeader {
            control_ring: ring,
            ..base
        }
        .encode();
        let error = parse_bootstrap(&page, 1 << 20)
            .expect_err("defective ring layout must not parse");
        assert_eq!(error, expected);
    }
}

#[test]
fn parser_survives_offset_capacity_wraparound() {
    let page = data_plane::bootstrap::BootstrapHeader {
        control_ring: ControlRingLayout {
            offset: u64::MAX - 8,
            capacity: 16,
            generation: 1,
        },
        arena_size: 1 << 20,
    }
    .encode();
    let error = parse_bootstrap(&page, 1 << 20).expect_err("wrapping ring must fail");
    assert_eq!(
        error,
        BootstrapError::RingOutOfBounds {
            offset: u64::MAX - 8,
            capacity: 16,
            arena_size: 1 << 20
        }
    );
}

/// Deterministic xorshift fuzz: arbitrary page bytes never panic, and a
/// successful parse implies every invariant (B4).
#[test]
fn parser_never_panics_on_arbitrary_pages() {
    let mut state = 0x9E3779B97F4A7C15_u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    for _ in 0..512 {
        let mut page = [0_u8; HEADER_LEN];
        for chunk in page.chunks_mut(8) {
            chunk.copy_from_slice(&next().to_le_bytes()[..chunk.len()]);
        }
        match parse_bootstrap(&page, 1 << 20) {
            Ok(resolved) => {
                let ring = resolved.control_ring;
                assert!(ring.capacity > 0);
                assert!(ring.generation > 0);
                assert!(ring.offset >= HEADER_END_OFFSET);
                assert!(ring
                    .offset
                    .checked_add(ring.capacity)
                    .is_some_and(|end| end <= resolved.arena_size));
            }
            Err(_) => {}
        }
    }
}

// ─── Writer ──────────────────────────────────────────────────────────────────

#[test]
fn writer_produces_parseable_header_and_disjoint_ring() {
    let mut arena = arena();
    let handoff = bootstrap::write_bootstrap(&mut arena, spec()).expect("bootstrap must write");

    let page = arena.read_arena(0, HEADER_LEN).expect("read header");
    assert_eq!(&page[0..4], b"SWBS", "magic bytes are frozen ABI");

    let resolved =
        parse_bootstrap(&page, arena.arena_len()).expect("written header must self-parse");
    assert_eq!(resolved.arena_size, arena.arena_len());
    assert_eq!(resolved.control_ring, handoff.control_ring);
    assert!(
        resolved.control_ring.offset >= HEADER_END_OFFSET,
        "control ring must not overlap the header region"
    );
    assert_eq!(
        resolved.control_ring.generation,
        bootstrap::CONTROL_RING_GENERATION
    );
}

#[test]
fn writer_leaves_ring_zeroed_with_generation_stamped() {
    let mut arena = arena();
    let handoff = bootstrap::write_bootstrap(&mut arena, spec()).expect("bootstrap must write");

    let ring = handoff.control_ring;
    let bytes = arena
        .read_arena(ring.offset, ring.capacity as usize)
        .expect("read ring region");
    assert_eq!(
        &bytes[0..8],
        &bootstrap::CONTROL_RING_GENERATION.to_le_bytes(),
        "generation is stamped at ring start"
    );
    assert!(
        bytes[8..].iter().all(|&b| b == 0),
        "rest of the fresh ring must be zero"
    );
}

#[test]
fn writer_emits_exactly_the_environment_abi() {
    use std::os::fd::AsRawFd;
    let mut arena = arena();
    let handoff = bootstrap::write_bootstrap(&mut arena, spec()).expect("bootstrap must write");

    let env = handoff.env;
    assert_eq!(
        env.keys().collect::<Vec<_>>(),
        vec![ENV_ARENA_FD, ENV_WAKE_FD],
        "B8: the two names are the whole env contract"
    );
    assert_eq!(
        env[ENV_ARENA_FD],
        handoff.arena_fd.as_raw_fd().to_string()
    );
    assert_eq!(env[ENV_WAKE_FD], handoff.wake_fd.as_raw_fd().to_string());
}

#[test]
fn writer_handoff_fds_are_distinct_and_open() {
    use std::fs::File;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;

    let mut arena = arena();
    let handoff = bootstrap::write_bootstrap(&mut arena, spec()).expect("bootstrap must write");

    let arena_fd = handoff.arena_fd.as_raw_fd();
    let wake_fd = handoff.wake_fd.as_raw_fd();
    let host_wake_fd = handoff.host_wake_fd.as_raw_fd();
    assert_ne!(arena_fd, wake_fd);
    assert_ne!(arena_fd, host_wake_fd);
    assert_ne!(wake_fd, host_wake_fd);

    // The wake descriptors must behave like eventfds: write 1, read counter.
    let mut wake = File::from(handoff.wake_fd);
    wake.write_all(&1_u64.to_le_bytes())
        .expect("eventfd write");
    let mut counter = [0_u8; 8];
    wake.read_exact(&mut counter).expect("eventfd read");
    assert_eq!(counter, 1_u64.to_le_bytes());
}

#[test]
fn writer_rejects_invalid_specs_and_oversized_rings() {
    let mut arena = arena();
    for bad in [
        bootstrap::ControlRingSpec {
            data_bytes: 0,
            alignment: 64,
        },
        bootstrap::ControlRingSpec {
            data_bytes: 4096,
            alignment: 0,
        },
    ] {
        let error = bootstrap::write_bootstrap(&mut arena, bad)
            .expect_err("invalid spec must fail before leasing");
        assert!(matches!(
            error,
            bootstrap::BootstrapWriteError::InvalidControlRingSpec
        ));
    }

    let oversized = bootstrap::ControlRingSpec {
        data_bytes: 1 << 21,
        alignment: 64,
    };
    let error = bootstrap::write_bootstrap(&mut arena, oversized)
        .expect_err("ring larger than the arena must fail");
    assert!(matches!(
        error,
        bootstrap::BootstrapWriteError::ControlRingLeaseRejected(_)
    ));
}
