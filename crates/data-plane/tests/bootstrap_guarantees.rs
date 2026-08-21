#![cfg(target_os = "linux")]

use std::collections::BTreeSet;
use std::os::fd::{AsRawFd, RawFd};

use data_plane::arena::{ArenaConfig, ArenaManager, NodeId};
use data_plane::bootstrap::{
    self, BOOTSTRAP_MAGIC, BOOTSTRAP_VERSION, BootstrapError, BootstrapHeader, ControlRegion,
    ENV_ARENA_FD, HEADER_END_OFFSET, HEADER_LEN, parse_bootstrap,
};
use data_plane::mapped_arena::MappedArena;

const ARENA_BYTES: u64 = 1 << 20;
const GENERATION: u64 = 41;

fn arena() -> ArenaManager {
    ArenaManager::boot(ArenaConfig {
        node_id: NodeId(7),
        reservation_ceiling: ARENA_BYTES,
        base_alignment: 64,
    })
    .expect("arena")
}

fn spec() -> bootstrap::BootstrapSpec {
    bootstrap::BootstrapSpec {
        arena_generation: GENERATION,
        alignment: 64,
    }
}

fn header() -> BootstrapHeader {
    BootstrapHeader {
        arena_size: ARENA_BYTES,
        arena_generation: GENERATION,
        control_region: None,
    }
}

fn fd_flags(fd: RawFd) -> i32 {
    // SAFETY: F_GETFD only inspects a live descriptor table entry.
    unsafe { libc::fcntl(fd, libc::F_GETFD) }
}

#[test]
fn v2_round_trip_contains_only_arena_identity() {
    let encoded = header().encode();
    let resolved = parse_bootstrap(&encoded, ARENA_BYTES).expect("valid bootstrap");

    assert_eq!(resolved.arena_size, ARENA_BYTES);
    assert_eq!(resolved.arena_generation, GENERATION);
    assert_eq!(resolved.control_region, None);
    assert_eq!(
        u32::from_le_bytes(encoded[0..4].try_into().unwrap()),
        BOOTSTRAP_MAGIC
    );
    assert_eq!(
        u16::from_le_bytes(encoded[4..6].try_into().unwrap()),
        BOOTSTRAP_VERSION
    );
    assert!(encoded[24..].iter().all(|byte| *byte == 0));
}

#[test]
fn private_control_region_is_bounds_checked_when_present() {
    let control = ControlRegion {
        offset: HEADER_END_OFFSET,
        length: 128,
    };
    let encoded = BootstrapHeader {
        control_region: Some(control),
        ..header()
    }
    .encode();

    assert_eq!(
        parse_bootstrap(&encoded, ARENA_BYTES)
            .expect("valid control region")
            .control_region,
        Some(control)
    );
}

#[test]
fn malformed_header_fails_before_attachment() {
    let encoded = header().encode();
    assert!(matches!(
        parse_bootstrap(&encoded[..HEADER_LEN - 1], ARENA_BYTES),
        Err(BootstrapError::Truncated { .. })
    ));

    let mut bad_magic = encoded;
    bad_magic[0] ^= 0xff;
    assert!(matches!(
        parse_bootstrap(&bad_magic, ARENA_BYTES),
        Err(BootstrapError::BadMagic { .. })
    ));

    let mut bad_version = encoded;
    bad_version[4..6].copy_from_slice(&(BOOTSTRAP_VERSION + 1).to_le_bytes());
    assert!(matches!(
        parse_bootstrap(&bad_version, ARENA_BYTES),
        Err(BootstrapError::UnsupportedVersion { .. })
    ));

    for reserved_at in [6, 47, 63] {
        let mut bad_reserved = encoded;
        bad_reserved[reserved_at] = 1;
        assert!(matches!(
            parse_bootstrap(&bad_reserved, ARENA_BYTES),
            Err(BootstrapError::ReservedBytesNotZero { at }) if at == reserved_at
        ));
    }

    assert!(matches!(
        parse_bootstrap(&encoded, ARENA_BYTES + 1),
        Err(BootstrapError::ArenaSizeMismatch { .. })
    ));

    let mut zero_generation = encoded;
    zero_generation[16..24].fill(0);
    assert!(matches!(
        parse_bootstrap(&zero_generation, ARENA_BYTES),
        Err(BootstrapError::ZeroArenaGeneration)
    ));
}

#[test]
fn malformed_control_geometry_is_rejected() {
    let mut partial = header().encode();
    partial[24..32].copy_from_slice(&HEADER_END_OFFSET.to_le_bytes());
    assert!(matches!(
        parse_bootstrap(&partial, ARENA_BYTES),
        Err(BootstrapError::PartialControlRegion { .. })
    ));

    let overlapping = BootstrapHeader {
        control_region: Some(ControlRegion {
            offset: HEADER_END_OFFSET - 1,
            length: 1,
        }),
        ..header()
    }
    .encode();
    assert!(matches!(
        parse_bootstrap(&overlapping, ARENA_BYTES),
        Err(BootstrapError::ControlOverlapsHeader { .. })
    ));

    for control in [
        ControlRegion {
            offset: ARENA_BYTES - 4,
            length: 8,
        },
        ControlRegion {
            offset: u64::MAX - 3,
            length: 8,
        },
    ] {
        let encoded = BootstrapHeader {
            control_region: Some(control),
            ..header()
        }
        .encode();
        assert!(matches!(
            parse_bootstrap(&encoded, ARENA_BYTES),
            Err(BootstrapError::ControlOutOfBounds { .. })
        ));
    }
}

#[test]
fn writer_exports_exactly_one_inheritable_descriptor() {
    let mut arena = arena();
    let handoff = bootstrap::write_bootstrap(&mut arena, spec()).expect("bootstrap write");

    assert_eq!(
        handoff
            .env
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([ENV_ARENA_FD])
    );
    assert_eq!(
        handoff.env[ENV_ARENA_FD],
        handoff.arena_fd.as_raw_fd().to_string()
    );
    assert_eq!(handoff.arena_generation, GENERATION);
    assert_eq!(fd_flags(handoff.arena_fd.as_raw_fd()) & libc::FD_CLOEXEC, 0);

    let page = arena.read_arena(0, HEADER_LEN).expect("read header");
    let resolved = parse_bootstrap(&page, arena.arena_len()).expect("written header parses");
    assert_eq!(resolved.arena_generation, GENERATION);
}

#[test]
fn mapped_arena_owns_mapping_but_closes_inherited_descriptor() {
    let mut host = arena();
    let handoff = bootstrap::write_bootstrap(&mut host, spec()).expect("bootstrap write");
    let inherited_fd = handoff.arena_fd.as_raw_fd();

    let (mapped, resolved) = MappedArena::map(handoff.arena_fd).expect("map arena");
    assert_eq!(mapped.len(), ARENA_BYTES as usize);
    assert_eq!(resolved.arena_generation, GENERATION);
    assert_eq!(
        fd_flags(inherited_fd),
        -1,
        "mapping closes the inherited descriptor"
    );

    host.write_arena(HEADER_END_OFFSET, b"visible")
        .expect("host write");
    let offset = HEADER_END_OFFSET as usize;
    // SAFETY: the asserted range lies inside the live mapping.
    let observed = unsafe { std::slice::from_raw_parts(mapped.base_ptr().add(offset), 7) };
    assert_eq!(observed, b"visible");
}

#[test]
fn writer_rejects_zero_generation_or_alignment() {
    for bad in [
        bootstrap::BootstrapSpec {
            arena_generation: 0,
            ..spec()
        },
        bootstrap::BootstrapSpec {
            alignment: 0,
            ..spec()
        },
    ] {
        assert!(matches!(
            bootstrap::write_bootstrap(&mut arena(), bad),
            Err(bootstrap::BootstrapWriteError::InvalidSpec)
        ));
    }
}
